# Helper for Moonraker to Klippy API calls.
#
# Copyright (C) 2020 Eric Callahan <arksine.code@gmail.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license.

from __future__ import annotations
import logging
import os
import time
import shutil
import asyncio
import re
from ..utils import Sentinel
from ..common import WebRequest, APITransport, RequestType
import configparser
from collections import deque

# Annotation imports
from typing import (
    TYPE_CHECKING,
    Any,
    Union,
    Optional,
    Dict,
    List,
    TypeVar,
    Mapping,
    Callable,
    Coroutine
)
if TYPE_CHECKING:
    from ..confighelper import ConfigHelper
    from ..common import UserInfo
    from .klippy_connection import KlippyConnection as Klippy
    from .file_manager.file_manager import FileManager
    Subscription = Dict[str, Optional[List[Any]]]
    SubCallback = Callable[[Dict[str, Dict[str, Any]], float], Optional[Coroutine]]
    _T = TypeVar("_T")

INFO_ENDPOINT = "info"
ESTOP_ENDPOINT = "emergency_stop"
LIST_EPS_ENDPOINT = "list_endpoints"
GC_OUTPUT_ENDPOINT = "gcode/subscribe_output"
GCODE_ENDPOINT = "gcode/script"
SUBSCRIPTION_ENDPOINT = "objects/subscribe"
STATUS_ENDPOINT = "objects/query"
OBJ_LIST_ENDPOINT = "objects/list"
REG_METHOD_ENDPOINT = "register_remote_method"

class KlippyAPI(APITransport):
    def __init__(self, config: ConfigHelper) -> None:
        self.server = config.get_server()
        self.klippy: Klippy = self.server.lookup_component("klippy_connection")
        self.fm: FileManager = self.server.lookup_component("file_manager")
        self.eventloop = self.server.get_event_loop()
        app_args = self.server.get_app_args()
        self.version = app_args.get('software_version')
        # Maintain a subscription for all moonraker requests, as
        # we do not want to overwrite them
        self.host_subscription: Subscription = {}
        self.subscription_callbacks: List[SubCallback] = []

        # Register GCode Aliases
        self.server.register_endpoint(
            "/printer/print/pause", RequestType.POST, self._gcode_pause
        )
        self.server.register_endpoint(
            "/printer/print/resume", RequestType.POST, self._gcode_resume
        )
        self.server.register_endpoint(
            "/printer/print/cancel", RequestType.POST, self._gcode_cancel
        )
        self.server.register_endpoint(
            "/printer/print/start", RequestType.POST, self._gcode_start_print
        )
        self.server.register_endpoint(
            "/printer/restart", RequestType.POST, self._gcode_restart
        )
        self.server.register_endpoint(
            "/printer/firmware_restart", RequestType.POST, self._gcode_firmware_restart
        )
        self.server.register_event_handler(
            "server:klippy_disconnect", self._on_klippy_disconnect
        )

        #diy start print
        self.if_prepare_finish = True
        self.up_z = 10
        self.fan_set_c = 100
        self.wait_for_sensor_ready = 40
        self.e_tempe_to_cool = 140
        self.b_tempe_to_cool = 40
        self.init_conf()

    def _on_klippy_disconnect(self) -> None:
        self.host_subscription.clear()
        self.subscription_callbacks.clear()

    async def _gcode_pause(self, web_request: WebRequest) -> str:
        return await self.pause_print()

    async def _gcode_resume(self, web_request: WebRequest) -> str:
        return await self.resume_print()

    async def _gcode_cancel(self, web_request: WebRequest) -> str:
        return await self.cancel_print()

    async def _gcode_start_print(self, web_request: WebRequest) -> str:
        filename: str = web_request.get_str('filename')
        user = web_request.get_current_user()
        return await self.start_print(filename, user=user)

    async def _gcode_restart(self, web_request: WebRequest) -> str:
        return await self.do_restart("RESTART")

    async def _gcode_firmware_restart(self, web_request: WebRequest) -> str:
        return await self.do_restart("FIRMWARE_RESTART")

    async def _send_klippy_request(
        self,
        method: str,
        params: Dict[str, Any],
        default: Any = Sentinel.MISSING,
        transport: Optional[APITransport] = None
    ) -> Any:
        try:
            req = WebRequest(method, params, transport=transport or self)
            result = await self.klippy.request(req)
        except self.server.error:
            if default is Sentinel.MISSING:
                raise
            result = default
        return result

    async def run_gcode(self,
                        script: str,
                        default: Any = Sentinel.MISSING
                        ) -> str:
        params = {'script': script}
        result = await self._send_klippy_request(
            GCODE_ENDPOINT, params, default)
        return result

    async def start_print(
        self,
        filename: str,
        wait_klippy_started: bool = False,
        user: Optional[UserInfo] = None
    ) -> str:
        # WARNING: Do not call this method from within the following
        # event handlers when "wait_klippy_started" is set to True:
        # klippy_identified, klippy_started, klippy_ready, klippy_disconnect
        # Doing so will result in "wait_started" blocking for the specifed
        # timeout (default 20s) and returning False.
        # XXX - validate that file is on disk
        logging.info(f"[startPrint_DEBUG]\tif_prepare_finish and check_can_print() 判断, if_prepare_finish = {self.if_prepare_finish}")
        if self.if_prepare_finish and await self.check_can_print():
            self.server.send_event("server:gcode_response", "preparing for print...")
            self.if_prepare_finish = False
            self.last_temps: Dict[str, Tuple[float, ...]] = {}
            self.temperature_store: TempStore = {}
            self.temp_store_size = 1200
            self.heater_bed_temp = 0.0
            self.extruder_temp = 0.0
            try:
                result: Dict[str, Any]
                result = await self.query_objects({'heaters': None})
            except self.server.error as e:
                logging.info(f"Error Configuring Sensors: {e}")
                self.if_prepare_finish = True
                return
            sensors: List[str]
            sensors = result.get("heaters", {}).get("available_sensors", [])
            if sensors:
                # Add Subscription
                sub: Dict[str, Optional[List[str]]] = {s: None for s in sensors}
                try:
                    status: Dict[str, Any]
                    status = await self.subscribe_objects(sub)
                except self.server.error as e:
                    logging.info(f"Error subscribing to sensors: {e}")
                    self.if_prepare_finish = True
                    return
                logging.info(f"Configuring available sensors: {sensors}")
                new_store: TempStore = {}
                for sensor in sensors:
                    fields = list(status.get(sensor, {}).keys())
                    if sensor in self.temperature_store:
                        new_store[sensor] = self.temperature_store[sensor]
                    else:
                        new_store[sensor] = {
                            'temperatures': deque(maxlen=self.temp_store_size)}
                        for item in ["target", "power", "speed"]:
                            if item in fields:
                                new_store[sensor][f"{item}s"] = deque(
                                    maxlen=self.temp_store_size)
                    if sensor not in self.last_temps:
                        self.last_temps[sensor] = (0., 0., 0., 0.)
                self.temperature_store = new_store
                # Prune unconfigured sensors in self.last_temps
                for sensor in list(self.last_temps.keys()):
                    if sensor not in self.temperature_store:
                        del self.last_temps[sensor]
                # Update initial temperatures
                for sensor in self.temperature_store:
                    if sensor in status:
                        last_val = self.last_temps[sensor]
                        if sensor == 'heater_bed':
                            self.heater_bed_temp = round(status[sensor].get('temperature', last_val[0]), 2)
                        if sensor == 'extruder':
                            self.extruder_temp = round(status[sensor].get('temperature', last_val[0]), 2)
            else:
                logging.info("No sensors found")
                self.last_temps = {}
                self.temperature_store = {}

            up_z_script = f'G1 Z{self.up_z} F600'
            await self.run_gcode('G28')
            await self.run_gcode(up_z_script)
            if self.extruder_temp >= self.e_tempe_to_cool or self.heater_bed_temp >= self.b_tempe_to_cool:
                fan_set_script = f'M106 S{int(((self.fan_set_c % 101) / 100 * 255) + 0.5)}'
                await self.run_gcode(fan_set_script)
                start_time = time.time()
                while True:
                    if time.time() - start_time > self.wait_for_sensor_ready:
                        break
                    time.sleep(1)
                    await asyncio.sleep(1)
            await self.run_gcode('M106 S0')
            self.server.send_event("server:gcode_response", "ready to start")
            self.if_prepare_finish = True
            if filename[0] == '/':
                filename = filename[1:]
            # Escape existing double quotes in the file name
            filename = filename.replace("\"", "\\\"")
            homedir = os.path.expanduser("~")
            if os.path.split(filename)[0].split(os.path.sep)[0] != ".cache" and re.match(r'sd[a-z]', os.path.split(filename)[0].split(os.path.sep)[0]):
                base_path = os.path.join(homedir, "printer_data/gcodes")
                target = os.path.join(".cache", os.path.basename(filename))
                cache_path = os.path.join(base_path, ".cache")
                if not os.path.exists(cache_path):
                    os.makedirs(cache_path)
                shutil.rmtree(cache_path)
                os.makedirs(cache_path)
                # metadata = self.fm.gcode_metadata.metadata.get(filename, None)
                try:
                    await self.copy_file_to_cache(os.path.join(base_path, filename), os.path.join(base_path, target), self.progress)
                except Exception as e:
                    logging.info(f"[startPrint_DEBUG]\tcopy_file_to_cache() Exception:{e}")
                    e_msg = f"Copy file to disk exception:{e}"
                    self.server.send_event("server:gcode_response", e_msg)
                    raise self.server.error(f"Copy file to disk exception:{e}", 507)
                # msg = "// metadata=" + json.dumps(metadata)
                # self.server.send_event("server:gcode_response", msg)
                filename = target
            script = f'SDCARD_PRINT_FILE FILENAME="{filename}"'
            if wait_klippy_started:
                await self.klippy.wait_started()
            logging.info(f"[startPrint_DEBUG]\tRequesting Job Start, filename = {filename}")
            ret = await self.run_gcode(script)
            self.server.send_event("klippy_apis:job_start_complete", user)
            return ret
        else:
            self.server.send_event("server:gcode_response", f"if_prepare_finish = {self.if_prepare_finish}")
            self.server.send_event("server:gcode_response", "!! SD busy")
            raise self.server.error("[startPrint]!! SD busy", 507)

    async def pause_print(
        self, default: Union[Sentinel, _T] = Sentinel.MISSING
    ) -> Union[_T, str]:
        self.server.send_event("klippy_apis:pause_requested")
        logging.info("Requesting job pause...")
        return await self._send_klippy_request(
            "pause_resume/pause", {}, default)

    async def resume_print(
        self, default: Union[Sentinel, _T] = Sentinel.MISSING
    ) -> Union[_T, str]:
        self.server.send_event("klippy_apis:resume_requested")
        logging.info("Requesting job resume...")
        return await self._send_klippy_request(
            "pause_resume/resume", {}, default)

    async def cancel_print(
        self, default: Union[Sentinel, _T] = Sentinel.MISSING
    ) -> Union[_T, str]:
        self.server.send_event("klippy_apis:cancel_requested")
        logging.info("Requesting job cancel...")
        return await self._send_klippy_request(
            "pause_resume/cancel", {}, default)

    async def do_restart(
        self, gc: str, wait_klippy_started: bool = False
    ) -> str:
        # WARNING: Do not call this method from within the following
        # event handlers when "wait_klippy_started" is set to True:
        # klippy_identified, klippy_started, klippy_ready, klippy_disconnect
        # Doing so will result in "wait_started" blocking for the specifed
        # timeout (default 20s) and returning False.
        if wait_klippy_started:
            await self.klippy.wait_started()
        try:
            result = await self.run_gcode(gc)
        except self.server.error as e:
            if str(e) == "Klippy Disconnected":
                result = "ok"
            else:
                raise
        return result

    async def list_endpoints(self,
                             default: Union[Sentinel, _T] = Sentinel.MISSING
                             ) -> Union[_T, Dict[str, List[str]]]:
        return await self._send_klippy_request(
            LIST_EPS_ENDPOINT, {}, default)

    async def emergency_stop(self) -> str:
        return await self._send_klippy_request(ESTOP_ENDPOINT, {})

    async def get_klippy_info(self,
                              send_id: bool = False,
                              default: Union[Sentinel, _T] = Sentinel.MISSING
                              ) -> Union[_T, Dict[str, Any]]:
        params = {}
        if send_id:
            ver = self.version
            params = {'client_info': {'program': "Moonraker", 'version': ver}}
        return await self._send_klippy_request(INFO_ENDPOINT, params, default)

    async def get_object_list(self,
                              default: Union[Sentinel, _T] = Sentinel.MISSING
                              ) -> Union[_T, List[str]]:
        result = await self._send_klippy_request(
            OBJ_LIST_ENDPOINT, {}, default)
        if isinstance(result, dict) and 'objects' in result:
            return result['objects']
        if default is not Sentinel.MISSING:
            return default
        raise self.server.error("Invalid response received from Klippy", 500)

    async def query_objects(self,
                            objects: Mapping[str, Optional[List[str]]],
                            default: Union[Sentinel, _T] = Sentinel.MISSING
                            ) -> Union[_T, Dict[str, Any]]:
        params = {'objects': objects}
        result = await self._send_klippy_request(
            STATUS_ENDPOINT, params, default)
        if isinstance(result, dict) and "status" in result:
            return result["status"]
        if default is not Sentinel.MISSING:
            return default
        raise self.server.error("Invalid response received from Klippy", 500)

    async def subscribe_objects(
        self,
        objects: Mapping[str, Optional[List[str]]],
        callback: Optional[SubCallback] = None,
        default: Union[Sentinel, _T] = Sentinel.MISSING
    ) -> Union[_T, Dict[str, Any]]:
        # The host transport shares subscriptions amongst all components
        for obj, items in objects.items():
            if obj in self.host_subscription:
                prev = self.host_subscription[obj]
                if items is None or prev is None:
                    self.host_subscription[obj] = None
                else:
                    uitems = list(set(prev) | set(items))
                    self.host_subscription[obj] = uitems
            else:
                self.host_subscription[obj] = items
        params = {"objects": dict(self.host_subscription)}
        result = await self._send_klippy_request(SUBSCRIPTION_ENDPOINT, params, default)
        if isinstance(result, dict) and "status" in result:
            if callback is not None:
                self.subscription_callbacks.append(callback)
            return result["status"]
        if default is not Sentinel.MISSING:
            return default
        raise self.server.error("Invalid response received from Klippy", 500)

    async def subscribe_from_transport(
        self,
        objects: Mapping[str, Optional[List[str]]],
        transport: APITransport,
        default: Union[Sentinel, _T] = Sentinel.MISSING,
        full_response: bool = False
    ) -> Union[_T, Dict[str, Any]]:
        params = {"objects": dict(objects)}
        result = await self._send_klippy_request(
            SUBSCRIPTION_ENDPOINT, params, default, transport
        )
        if isinstance(result, dict) and "status" in result:
            if full_response:
                return result
            return result["status"]
        if default is not Sentinel.MISSING:
            return default
        raise self.server.error("Invalid response received from Klippy", 500)

    async def subscribe_gcode_output(self) -> str:
        template = {'response_template':
                    {'method': "process_gcode_response"}}
        return await self._send_klippy_request(GC_OUTPUT_ENDPOINT, template)

    async def register_method(self, method_name: str) -> str:
        return await self._send_klippy_request(
            REG_METHOD_ENDPOINT,
            {'response_template': {"method": method_name},
             'remote_method': method_name})

    def send_status(
        self, status: Dict[str, Any], eventtime: float
    ) -> None:
        for cb in self.subscription_callbacks:
            self.eventloop.register_callback(cb, status, eventtime)
        self.server.send_event("server:status_update", status)

    async def copy_file_to_cache(self, origin, target, callback=None):
        stat = os.statvfs("/")
        free_space = stat.f_frsize * stat.f_bfree
        filesize = os.path.getsize(origin)
        # if True :
        if free_space - filesize < ( 100 * 1024 *1024 ) :
            msg = "!! Insufficient disk space, unable to read the file."
            self.server.send_event("server:gcode_response", msg)
            raise self.server.error("Insufficient disk space, unable to read the file.", 507)

        with open(origin, 'rb') as fsrc, open(target, 'wb') as fdst:
            loop = asyncio.get_running_loop()
            copied = 0
            last_time = time.time()
            while True:
                await asyncio.sleep(0.1)
                buf = await loop.run_in_executor(None, fsrc.read, 1024 * 1024 * 4)
                if not buf:
                    self.server.send_event("server:gcode_response", f"Copying gcode file from USB DISK: copied [100%]")
                    break
                await loop.run_in_executor(None, fdst.write, buf)
                copied += len(buf)
                current_time = time.time()
                if callback and (current_time - last_time >= 1):
                    await callback(copied, filesize) 
                    last_time = current_time

    async def progress(self, copied, total):
        # self.server.send_event("server:gcode_response", f"Copying gcode file from USB DISK: copied {copied} of {total} bytes ({copied / total:.2%})")
        self.server.send_event("server:gcode_response", f"Copying gcode file from USB DISK: copied [{copied / total:.2%}]")
    async def check_can_print(self) -> bool:
        try:
            result = await self.query_objects({"print_stats": None})
        except Exception:
            # Klippy not connected
            return False
        if 'print_stats' not in result:
            return False
        state: str = result['print_stats']['state']
        if state in ["printing", "paused"]:
            return False
        return True
    def init_conf(self):
        config = configparser.ConfigParser()
        conf_ini_path_l = [47, 104, 111, 109, 101, 47, 109, 107, 115, 47, 68, 101, 115, 107, 116, 111, 112, 47, 109, 121, 102, 105, 108, 101, 47, 122, 110, 112, 47, 122, 110, 112, 95, 116, 106, 99, 95, 107, 108, 105, 112, 112, 101, 114, 47, 101, 108, 101, 103, 111, 111, 95, 99, 111, 110, 102, 46, 105, 110, 105]
        conf_ini_path = "".join(map(chr, conf_ini_path_l))
        config.read(conf_ini_path)

        logging.info("load level mode conf")
        if config.has_section("level_mode"):
            if config.has_option("level_mode", "up_z"):
                self.up_z = config.getint("level_mode", "up_z")
            if config.has_option("level_mode", "fan_set_c"):
                self.fan_set_c = config.getint("level_mode", "fan_set_c")
            if config.has_option("level_mode", "wait_for_sensor_ready"):
                self.wait_for_sensor_ready = config.getint("level_mode", "wait_for_sensor_ready")
            if config.has_option("level_mode", "e_tempe_to_cool"):
                self.e_tempe_to_cool = config.getint("level_mode", "e_tempe_to_cool")
            if config.has_option("level_mode", "b_tempe_to_cool"):
                self.b_tempe_to_cool = config.getint("level_mode", "b_tempe_to_cool")
    def reset_some_status(self):
        self.if_prepare_finish = True#reset self.if_prepare_finish for start_print()

def load_component(config: ConfigHelper) -> KlippyAPI:
    return KlippyAPI(config)

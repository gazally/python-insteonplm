""" MQTT service for python-insteonplm. """
import argparse
import asyncio
import certifi
import json
import logging
import os
from queue import Queue
import sys

import paho.mqtt.client as mqtt
import insteonplm
from insteonplm.address import Address
from insteonplm.devices import create, ALDBStatus

__all__ = ("MQTTLink", "run_mqtt")
logger = logging.getLogger(__name__)
BUFSIZE = 100

class MQTTLink:
    """Make python-insteonplm speak MQTT."""

    def __init__(self, loop, args=None):
        # common variables
        self.loop = loop
        self.plm = insteonplm.PLM()
        self.logfile = None
        self.workdir = None
        self.loglevel = logging.INFO

        # connection variables
        self.device = args.device
        self.username = None
        self.password = None
        self.host = None
        self.port = None

        # MQTT variables
        username = os.getenv("MQTT_USER")
        password = os.getenv("MQTT_PASSWORD")
        self.client = mqtt.Client()
        self.topic = args.topic
        if username is not None:
            self.client.username_pw_set(username, password)
        if args.use_tls:
            self.client.tls_set(certifi.where())
        self.client.on_connect = self.on_mqtt_connect

        self.command_queue = Queue(BUFSIZE)

        self.aldb_load_lock = asyncio.Lock()

        if args:
            if args.verbose:
                self.loglevel = logging.DEBUG
            else:
                self.loglevel = logging.INFO

            if hasattr(args, "workdir"):
                self.workdir = args.workdir
            if hasattr(args, "logfile"):
                self.logfile = args.logfile

        if self.logfile:
            logging.basicConfig(level=self.loglevel, filename=self.logfile)
        else:
            handler = logging.StreamHandler(sys.stdout)
            logger.addHandler(handler)
            logger.info("Setting log level to %s", self.loglevel)
            logger.setLevel(self.loglevel)
            logging.getLogger("insteonplm").setLevel(self.loglevel)

        self.tasks = set()
        self.ready = False

    def start(self):
        task = self.loop.create_task(self.connect())
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

        task = self.loop.create_task(self.exec_commands_from_queue())
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

        logger.info("Calling MQTT connect")
        self.client.connect(args.broker, args.port)
        self.client.will_set(topic="/".join((self.topic, "status")), payload="offline", qos=1, retain=True)
        self.client.loop_start()

    def close(self):
        if self.plm:
            if self.plm.transport:
                logger.info('Closing the session')
                self.plm.transport.close()
            task = self.loop.create_task(self.plm.close())
            self.loop.run_until_complete(task)

    async def connect(self, poll_devices=False, device=None, workdir=None):
        """Connect to the IM."""
        await self.aldb_load_lock.acquire()
        device = self.host if self.host else self.device
        logger.info("Connecting to Insteon Modem at %s", device)
        self.device = device if device else self.device
        self.workdir = workdir if workdir else self.workdir
        conn = await insteonplm.Connection.create(
            device=self.device,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            loop=self.loop,
            poll_devices=poll_devices,
            workdir=self.workdir,
        )
        logger.info("Connection made to Insteon Modem at %s", device)
        conn.protocol.add_device_callback(self.async_new_device_callback)
        conn.protocol.add_all_link_done_callback(self.async_aldb_loaded_callback)
        await self.aldb_load_lock.acquire()
        self.plm = conn.protocol
        if self.aldb_load_lock.locked():
            self.aldb_load_lock.release()

    def async_new_device_callback(self, device):
        """Log that our new device callback worked."""
        logger.info(
            "New Device: %s cat: 0x%02x subcat: 0x%02x desc: %s, model: %s",
            device.id,
            device.cat,
            device.subcat,
            device.description,
            device.model,
        )
        for state in device.states:
            device.states[state].register_updates(self.async_state_change_callback)
            logger.info(
                "Device: %s:%x New state registered: %s",
                device.id,
                state,
                device.states[state].name,
            )
        self.client.publish(topic="/".join((self.topic, "device", device.id, "info")),
                            payload=json.dumps({"cat": device.cat,
                                                "subcat": device.subcat,
                                                "description": device.description,
                                                "model": device.model,
                                                "states": {k: device.states[k].name for k in device.states}}),
                            qos=1, retain=True)


    def async_state_change_callback(self, addr, state, value):
        """Log the state change."""
        logger.info(
            "Device %s state %s value is changed to %s", addr.human, state, value
        )
        self.client.publish(topic="/".join((self.topic, "device", addr.hex, "update", str(state))), payload=value,
                            qos=1, retain=True)

    def async_aldb_loaded_callback(self):
        """Unlock the ALDB load lock when loading is complete."""
        if self.aldb_load_lock.locked():
            self.aldb_load_lock.release()
        self.client.publish(topic="/".join((self.topic, "insteon_status")), payload="ready", qos=1, retain=True)
        self.ready = True
        logger.info("ALDB Loaded")

    def device_set_level(self, addr, level, group):
        """Set the level of a device.

        Usage:
            device_set_level address level [group]
        Arguments:
            address: Required - INSTEON address of the device
            level: Required
            group: Optional - All-Link group number. Defaults to 1
        """
        logger.info("device_set_level %s %s %s", addr, level, group)
        device = None
        state = None

        if addr:
            dev_addr = Address(addr)
            device = self.plm.devices[dev_addr.id]

        if device:
            state = device.states[group]
            if state:
                try:
                    logger.info(
                        "Set to device %s:0x%x to %s", dev_addr.human, group, level
                    )
                    state.set_level(level)
                except AttributeError:
                    logger.warning(
                        "device %s:%s-%s state %s: does not " "support set-level command",
                        state.address.human,
                        device.model,
                        device.description,
                        state.name,
                    )
            else:
                logger.warning(
                    "device %s:%s-%s does not have group %s",
                    device.address.human,
                    device.model,
                    device.description,
                    group,
                )
        else:
            logger.warning("device %s does not exist", addr)

    def device_request_update(self, addr, state_index):
        """Request an updated state value from a device.
        """
        logger.info("device_request_update %s %s", addr, state_index)
        device = None
        state = None

        if addr:
            dev_addr = Address(addr)
            device = self.plm.devices[dev_addr.id]

        if device:
            state = device.states[state_index]
            if state:
                try:
                    logger.info(
                        "Update device state %s:0x%x", dev_addr.human, state_index
                    )
                    state.async_refresh_state()
                except AttributeError:
                    logger.warning(
                        "device %s:%s-%s  state %s: does not " "support request_update command",
                        state.address.human,
                        device.model,
                        device.description,
                        state.name,
                    )
            else:
                logger.warning(
                    "device %s:%s-%s does not have state %s",
                    device.address.human,
                    device.model,
                    device.description,
                    state_index,
                )
        else:
            logger.warning("device %s does not exist", addr)

    def on_mqtt_connect(self, client, userdata, flags, rc):
        logger.info("MQTT connected rc=%s", rc)
        self.client.on_message = self.on_mqtt_message
        self.client.publish(topic="/".join((self.topic, "status")), payload="online", qos=1, retain=True)
        self.client.publish(topic="/".join((self.topic, "insteon_status")), payload="ready" if self.ready else "not ready", qos=1, retain=True)
        self.client.subscribe("/".join((self.topic, "command", "#")), qos=1)

    def on_mqtt_message(self, client, userdata, message):
        logger.info("MQTT message: %s %s", message.topic, message.payload)
        try:
            payload = json.loads(message.payload)
            self.command_queue.put((message.topic.split("/")[-1], payload))
        except json.JSONDecodeError:
            logger.error("Invalid JSON in payload: %s %s", message.topic, message.payload)

    async def exec_commands_from_queue(self):
        try:
            while True:
                command, payload = await self.loop.run_in_executor(None, self.command_queue.get)
                await self.execute_command(command, payload)
        except asyncio.CancelledError:
            # Unblock the command_queue.get which is running in another thread.
            # Otherwise the program won't exit.
            self.command_queue.put(None)
            logger.info("Shutting down MQTT queue listener")

    async def execute_command(self, command, payload):
        logger.info("MQTT command: %s %s", command, payload)

        if command == "set_level":
            self.do_set_level(payload)
        elif command == "request_update":
            self.do_request_updated_value(payload)
        else:
            logger.info("MQTT: unknown command %s", command)

    def do_set_level(self, args):
        """Set the level of a device.

        payload should contain the keys "device" and "level" and
        optionally "group" which defaults to 1.  "group" seems to be
        the state number.

        """
        addr = args.get("device", None)
        level = args.get("level", None)
        group = args.get("group", 1)

        if addr and level and group:
            self.device_set_level(addr, level, group)
        else:
            logger.error("set_level: Invalid device, level, or group in %s", args)

    def do_request_updated_value(self, args):
        """ Request the current value of a device state.

        payload should contain the keys "device" (hex address), and
        "state" (integer state index).
       """
        addr = args.get("device", None)
        state_index = args.get("state", None)

        if addr and state_index:
            self.device_request_update(addr, state_index)
        else:
            logger.error("request_update: Invalid device or state_index in %s", args)


def run_mqtt():
    """Create a event loop to handle MQTT publishing and subscribing.

    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--device", default="/dev/ttyUSB0", help="Path to PLM device")
    parser.add_argument(
        "--verbose", "-v", action="count", help="Set logging level to verbose"
    )
    parser.add_argument("-l", "--logfile", default="", help="Log file name")
    parser.add_argument(
        "--workdir",
        default="",
        help="Working directory for reading and saving " "device information.",
    )
    parser.add_argument("--broker", default="mqtt.eclipse.org", help="URL of MQTT broker")
    parser.add_argument("--port", type=int, default=1833, help="Port used by MQTT broker")
    parser.add_argument("--use-tls", type=bool, default=False, help="Connect to MQTT broker using TLS")
    parser.add_argument("--topic", default="insteon", help="MQTT topic to publish to")

    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    mqttlink = MQTTLink(loop, args)
    mqttlink.start()

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        mqttlink.close()
        pending = asyncio.all_tasks(loop=loop)
        for task in pending:
            task.cancel()
            try:
                loop.run_until_complete(task)
            except asyncio.CancelledError:
                pass
            except KeyboardInterrupt:
                pass

        pending = asyncio.all_tasks(loop=loop)
    finally:
        loop.stop()
        loop.close()



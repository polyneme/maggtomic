from collections import deque
from copy import deepcopy

from toolz import assoc_in, concatv, get_in, merge


def listify(thing):
    return thing if isinstance(thing, list) else [thing]


def assign(assigner_map):
    return {
        "type": "assign",
        "exec": lambda context, event: {
            k: v(context, event) for k, v in assigner_map.items()
        },
    }


def raise_event(e):
    """Add event to front of queue. If argument == "@this", re-raise current event."""
    return {
        "type": "raise",
        "exec": lambda context, event: event if e == "@this" else e,
    }


def transact(context, tx_data: list):
    """Extend "data" item of context with transaction data."""
    return list(concatv(context["data"], tx_data))


def dpa_pass(context, event):
    """Dummy property assigned function that returns "data" item of context."""
    return context["data"]


class Machine:
    def __init__(self, config, options, initial_context=None):
        self.config = config
        self.options = options
        self.state = {
            "value": self.config["initial"],
            "context": initial_context or {},
            "actions": [],
        }
        self.initial_context = deepcopy(self.state["context"])

    def get_reset_copy(self):
        return Machine(self.config, self.options, self.initial_context)

    def guard(self, name):
        return get_in(["guards", name], self.options)

    def action(self, name):
        return get_in(["actions", name], self.options)

    def with_context(self, context, set_as_initial_context=False):
        self.state = assoc_in(self.state, ["context"], context)
        if set_as_initial_context:
            self.initial_context = deepcopy(self.state["context"])

    def transition(self, state, event):
        """Return description of next state without executing actions or transition."""
        if isinstance(state, str):
            state = merge(self.state, {"value": state})
        transition = get_in(
            ["states", state["value"], "on", event["type"]], self.config
        )
        if transition is None:
            return state

        transition = listify(transition)
        for option in transition:
            if "cond" in option:
                guard = self.guard(option["cond"])
                if guard is None:
                    print(option)
                if guard(state["context"], event):
                    return merge(
                        state,
                        {
                            "value": option["target"],
                            "actions": option.get("actions", []),
                        },
                    )
            else:
                return merge(
                    state,
                    {"value": option["target"], "actions": option.get("actions", [])},
                )
        return state


class Service:
    def __init__(self, machine: Machine):
        self.machine = machine
        self.events = deque()
        self.active = False

    def start(self, at_state=None):
        """respond to sent events"""
        self.active = True
        self.process_events()

    def stop(self):
        """queue but do not respond to events"""
        self.active = False

    def send(self, events):
        """register event to be processed by machine"""
        self.events.extend(listify(events))
        if self.active:
            self.process_events()

    def process_events(self):
        while self.events:
            event = self.events.popleft()
            next_state = self.machine.transition(self.machine.state, event)
            for name in next_state.get("actions", []):
                action = self.machine.action(name)
                if not action:
                    print("can't find action", name)
                if action["type"] == "assign":
                    next_state["context"] = merge(
                        next_state["context"],
                        action["exec"](self.machine.state["context"], event),
                    )
                elif action["type"] == "raise":
                    event_to_raise = action["exec"](
                        self.machine.state["context"], event
                    )
                    self.events.appendleft(event_to_raise)
            next_state["actions"] = []
            self.machine.state = next_state


def interpret(machine: Machine):
    """Return a service to interpret this machine."""
    return Service(machine)


def event_sequence_for(path, last_modified, component_getter=None):
    fullpath, components = component_getter(path)
    events = [
        {"type": "PATH", "data": {"path": fullpath, "last_modified": last_modified}}
    ]
    events.extend([{"type": "PATH_COMPONENT", "data": c} for c in components])
    return events


def event_data_in(s):
    def wrapped(context, event):
        return event["data"] in s

    return wrapped


def event_data_pred(p):
    def wrapped(context, event):
        return p(event["data"])

    return wrapped


def event_data_suffix_in(s, delim="."):
    def wrapped(context, event):
        return event["data"].split(delim)[-1] in s

    return wrapped


def run_machine_foreach(machine, manifest, component_getter=None):
    machines = []
    for key, ts in manifest.items():
        m = machine.get_reset_copy()
        service = interpret(m)
        service.start()
        for event in event_sequence_for(key, ts, component_getter):
            service.send(event)
        machines.append(m)
    return machines

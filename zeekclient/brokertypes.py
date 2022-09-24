"""A Python type hierarchy representing Broker's data model.

Supported types are placed in a type hierarchy, with each type supporting
serialization to Broker's WebSocket wire format, unserialization from it, and
creation from "native" Python values.

For reference, see: https://docs.zeek.org/projects/broker/en/current/web-socket.html
"""
import abc
import enum
import ipaddress
import json
import re

class Type(abc.ABC):
    """Base class for types we can instantiate from or render to Broker's JSON
    data model. For details, see:
    https://docs.zeek.org/projects/broker/en/current/web-socket.html
    """
    def serialize(self, pretty=False):
        """Serializes the object to Broker-compatible wire data.

        pretty: When True, pretty-prints the resulting JSON.

        Returns: raw message data ready to transmit.
        """
        indent = 4 if pretty else None
        return json.dumps(self.to_broker(), indent=indent, sort_keys=True)

    def __eq__(self, other):
        """The default equality method for brokertypes.

        This implements member-by-member comparison based on the object's
        __dict__. The types complement this by each implementing their own
        __hash__() method.
        """
        if type(self) != type(other):
            return NotImplemented
        if len(self.__dict__) != len(other.__dict__):
            return False
        for attr in self.__dict__:
            if self.__dict__[attr] != other.__dict__[attr]:
                return False
        return True

    def __repr__(self):
        return self.serialize()

    def __str__(self):
        return self.serialize(pretty=True)

    @classmethod
    def unserialize(cls, data): # pylint: disable=unused-argument
        """Instantiates an object of this class from Broker wire data.

        This assumes the message content in JSON and first unserializes it into
        a Python data structure. It then calls from_broker() to instantiate an
        object of this class from it.

        data: raw wire WebSocket message content

        Returns: the resulting brokertype object.

        Raises: TypeError in case of invalid data. The exception's message
        provides details.
        """
        try:
            obj = json.loads(data)
        except json.JSONDecodeError as err:
            raise TypeError('cannot parse JSON data for {}: {} -- {}'.format(
                cls.__name__, err.msg, data)) from err

        cls.check_broker_data(obj)

        try:
            # This may raise TypeError directly, which we pass on to the caller
            return cls.from_broker(obj)
        except (IndexError, KeyError, ValueError) as err:
            raise TypeError('invalid data for {}: {}'.format(
                cls.__name__, data)) from err

    @abc.abstractmethod
    def to_broker(self):  # pylint: disable=no-self-use
        """Returns a Broker-JSON-compatible Python data structure representing
        a value of this type.
        """
        return None

    @abc.abstractmethod
    def to_py(self):  # pylint: disable=no-self-use
        """Returns a Python-"native" rendering of the object.

        For most brokertypes this will be a native Python type (such as int or
        str), but for some types the closest thing to a natural rendering of the
        value in Python will be the object itself.

        Return: a Python value
        """
        return None

    @classmethod
    @abc.abstractmethod
    def check_broker_data(cls, data): # pylint: disable=unused-argument
        """Checks the Broker data for compliance with the expected type.

        If you use unserialize() to obtain objects, you can ignore this
        method. The module invokes it under the hood.

        data: a Python data structure resulting from json.loads().

        Raises TypeError in case of problems.
        """

    @classmethod
    @abc.abstractmethod
    def from_broker(cls, data): # pylint: disable=unused-argument
        """Returns an instance of the type given Broker's JSON data.

        This is a low-level method that you likely don't want to use. Consider
        unserialize() instead: it handles raw wire data unserialization,
        type-checking, and exception canonicalization.

        data: a JSON-unserialized Python data structure.

        Raises: type-specific exceptions resulting from value construction, such
        as TypeError, KeyError, or ValueError.
        """
        return None


# ---- Basic types -----------------------------------------------------

class DataType(Type):
    """Base class for data types known to Broker."""
    def __lt__(self, other):
        if not isinstance(other, DataType):
            raise TypeError("'<' comparison not supported between instances "
                            "of '{}' and '{}'".format(type(self).__name__,
                                                      type(other).__name__))
        # Supporting comparison accross data types allows us to sort the members
        # of a set or table keys. We simply compare the type names:
        if type(self) != type(other):
            return type(self).__name__ < type(other).__name__

        return NotImplemented

    @classmethod
    def check_broker_data(cls, data):
        if not isinstance(data, dict):
            raise TypeError('invalid data layout for Broker data: not an object')
        if '@data-type' not in data or 'data' not in data:
            raise TypeError('invalid data layout for Broker data: required keys missing')

class NoneType(DataType):
    """Broker's representation of an absent value."""
    def __init__(self, _=None):
        # It helps to have a constructor that can be passed None explicitly, for
        # symmetry with other constructors below.
        pass

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        return False

    def __hash__(self):
        return hash(None)

    def to_broker(self):
        return {
            '@data-type': 'none',
            'data': {},
        }

    def to_py(self):
        return None

    @classmethod
    def from_broker(cls, data):
        return NoneType()


class Boolean(DataType):
    def __init__(self, value):
        self._value = bool(value)

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        return self._value < other._value

    def __hash__(self):
        return hash(self._value)

    def to_broker(self):
        return {
            '@data-type': 'boolean',
            'data': self._value,
        }

    def to_py(self):
        return self._value

    @classmethod
    def from_broker(cls, data):
        return Boolean(data['data'])


class Count(DataType):
    def __init__(self, value):
        self._value = int(value)
        if self._value < 0:
            raise ValueError('Count can only hold non-negative values')

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        return self._value < other._value

    def __hash__(self):
        return hash(self._value)

    def to_broker(self):
        return {
            '@data-type': 'count',
            'data': self._value,
       }

    def to_py(self):
        return self._value

    @classmethod
    def from_broker(cls, data):
        return Count(data['data'])


class Integer(DataType):
    def __init__(self, value):
        self._value = int(value)

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        return self._value < other._value

    def __hash__(self):
        return hash(self._value)

    def to_py(self):
        return self._value

    def to_broker(self):
        return {
            '@data-type': 'integer',
            'data': self._value,
        }

    @classmethod
    def from_broker(cls, data):
        return Integer(data['data'])


class Real(DataType):
    def __init__(self, value):
        self._value = float(value)

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        return self._value < other._value

    def __hash__(self):
        return hash(self._value)

    def to_py(self):
        return self._value

    def to_broker(self):
        return {
            '@data-type': 'real',
            'data': self._value,
        }

    @classmethod
    def from_broker(cls, data):
        return Real(data['data'])


class Timespan(DataType):
    RE = re.compile(r'(\d+)(ns|ms|s|min|h|d)')

    class Unit(enum.Enum):
        NS = 'ns'
        MS = 'ms'
        S = 's'
        MIN = 'min'
        H = 'h'
        D = 'd'

    def __init__(self, value, unit):
        self._value = int(value) # Broker uses integers for the numeric part
        self._unit = unit
        if not isinstance(unit, self.Unit):
            raise TypeError('Timespan constructor requires Unit enum')

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        return self.to_secs() < other.to_secs()

    def __hash__(self):
        return hash((self._value, self._unit))

    def to_secs(self):
        if self._unit == self.Unit.NS:
            return self._value * 1e-06
        if self._unit == self.Unit.MS:
            return self._value * 1e-03
        if self._unit == self.Unit.S:
            return float(self._value)
        if self._unit == self.Unit.MIN:
            return float(self._value * 60)
        if self._unit == self.Unit.H:
            return float(self._value * 60 * 60)
        if self._unit == self.Unit.D:
            return float(self._value * 60 * 60 * 24)
        assert False, "unhandled timespan unit '{}'".format(self._unit)

    def to_broker(self):
        return {
            '@data-type': 'timestpan',
            'data': '{}{}'.format(self._value, self._unit.value)
        }

    def to_py(self):
        return self

    @classmethod
    def from_broker(cls, data):
        mob = cls.RE.fullmatch(data['data'])
        return Timespan(mob[1], Timespan.Unit(mob[2]))


class String(DataType):
    def __init__(self, value):
        self._value = str(value)

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        return self._value < other._value

    def __hash__(self):
        return hash(self._value)

    def to_broker(self):
        return {
            '@data-type': 'string',
            'data': self._value,
        }

    def to_py(self):
        return self._value

    @classmethod
    def from_broker(cls, data):
        return String(data['data'])


class Enum(DataType):
    def __init__(self, value):
        self._value = str(value)

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        return self._value < other._value

    def __hash__(self):
        return hash(self._value)

    def to_broker(self):
        return {
            '@data-type': 'enum-value',
            'data': self._value,
        }

    def to_py(self):
        return self._value

    @classmethod
    def from_broker(cls, data):
        return Enum(data['data'])


class Address(DataType):
    def __init__(self, value):
        self._value = str(value)
        # Throws ValueError when value is not v4/v6:
        ipaddress.ip_address(self._value)

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        return self._value < other._value

    def __hash__(self):
        return hash(self._value)

    def to_broker(self):
        return {
            '@data-type': 'address',
            'data': self._value,
        }

    def to_py(self):
        return self._value

    @classmethod
    def from_broker(cls, data):
        return Address(data['data'])


class Port(DataType):
    class Proto(enum.Enum):
        UNKNOWN = '?'
        TCP = 'tcp'
        UDP = 'udp'
        ICMP = 'icmp'

    def __init__(self, number, proto=Proto.TCP):
        self.number = int(number)
        self.proto = proto
        if not isinstance(proto, self.Proto):
            raise TypeError('Port constructor requires Proto enum')
        if self.number < 1 or self.number > 65535:
            raise ValueError("Port number '{}' invalid".format(self.number))

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        order = ['?', 'tcp', 'udp', 'icmp']
        if order.index(self.proto.value) < order.index(other.proto.value):
            return True
        return self.number < other.number

    def __hash__(self):
        return hash((self.number, self.proto))

    def to_broker(self):
        return {
            '@data-type': 'port',
            'data': '{}/{}'.format(self.number, self.proto.value),
        }

    def to_py(self):
        return self

    @classmethod
    def from_broker(cls, data):
        return Port(data['data'].split('/', 1)[0],
                    Port.Proto(data['data'].split('/', 1)[1]))


class Vector(DataType):
    def __init__(self, elements=None):
        self._elements = elements or []
        if not isinstance(self._elements, tuple) and not isinstance(self._elements, list):
            raise TypeError('Vector initialization requires tuple or list data')
        if not all(isinstance(elem, Type) for elem in self._elements):
            raise TypeError('Non-empty Vector construction requires brokertype values.')

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        for el1, el2 in zip(self._elements, other._elements):
            if el1 < el2:
                return True
        if len(self._elements) < len(other._elements):
            return True
        return False

    def __hash__(self):
        return hash(tuple(self._elements))

    def __iter__(self):
        return iter(self._elements)

    def __len__(self):
        return len(self._elements)

    def __getitem__(self, idx):
        return self._elements[idx]

    def to_broker(self):
        return {
            '@data-type': 'vector',
            'data': [elem.to_broker() for elem in self._elements],
        }

    def to_py(self):
        return [elem.to_py() for elem in self._elements]

    @classmethod
    def from_broker(cls, data):
        res = Vector()
        for elem in data['data']:
            res._elements.append(from_broker(elem))
        return res


class Set(DataType):
    def __init__(self, elements=None):
        self._elements = elements or set()
        if not isinstance(self._elements, set):
            raise TypeError('Set initialization requires set data')
        if not all(isinstance(elem, Type) for elem in self._elements):
            raise TypeError('Non-empty Set construction requires brokertype values.')

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        for el1, el2 in zip(sorted(self._elements), sorted(other._elements)):
            if el1 < el2:
                return True
        if len(self._elements) < len(other._elements):
            return True
        return False

    def __hash__(self):
        return hash(tuple(sorted(self._elements)))

    def __iter__(self):
        return iter(self._elements)

    def __len__(self):
        return len(self._elements)

    def __contains__(self, key):
        return key in self._elements

    def to_broker(self):
        return {
            '@data-type': 'set',
            'data': [elem.to_broker() for elem in sorted(self._elements)],
        }

    def to_py(self):
        return set(elem.to_py() for elem in self._elements)

    @classmethod
    def from_broker(cls, data):
        res = Set()
        for elem in data['data']:
            res._elements.add(from_broker(elem))
        return res


class Table(DataType):
    def __init__(self, elements=None):
        self._elements = elements or {}
        if not isinstance(self._elements, dict):
            raise TypeError('Table initialization requires dict data')
        keys_ok = all(isinstance(elem, Type) for elem in self._elements.keys())
        vals_ok = all(isinstance(elem, Type) for elem in self._elements.values())
        if not keys_ok or not vals_ok:
            raise TypeError('Non-empty Table construction requires brokertype values.')

    def __lt__(self, other):
        res = super().__lt__(other)
        if res != NotImplemented:
            return res
        for key1, key2 in zip(sorted(self._elements), sorted(other._elements)):
            if key1 < key2:
                return True
            if self._elements[key1] < other._elements[key2]:
                return True
        if len(self._elements) < len(other._elements):
            return True
        return False

    def __hash__(self):
        return hash((key, self._elements[key]) for key in sorted(self._elements))

    def __iter__(self):
        return iter(self._elements)

    def __len__(self):
        return len(self._elements)

    def __contains__(self, key):
        return key in self._elements

    def keys(self):
        return self._elements.keys()

    def values(self):
        return self._elements.values()

    def items(self):
        return self._elements.items()

    def to_broker(self):
        return {
            '@data-type': 'table',
            'data': [{'key': key.to_broker(), 'value': self._elements[key].to_broker()}
                     for key in sorted(self._elements)]
        }

    def to_py(self):
        res = {}
        for key, val in self._elements.items():
            res[key.to_py()] = val.to_py()
        return res

    @classmethod
    def from_broker(cls, data):
        res = Table()
        for elem in data['data']:
            res._elements[from_broker(elem['key'])] = from_broker(elem['value'])
        return res


# ---- Special types ---------------------------------------------------

class ZeekEvent(Vector):
    """Broker's event representation, as a vector of vectors.

    This specialization isn't an official type in Broker's hierarchy: there's no
    distinguishing @data-type for it. Zeek events are a specific interpretation
    of nested vectors.

    See Broker's websockets docs for an example:

    https://docs.zeek.org/projects/broker/en/current/web-socket.html#encoding-of-zeek-events
    """
    def __init__(self, name, *args):
        super().__init__()

        self.name = name.to_py() if isinstance(name, String) else str(name)
        self.args = list(args) or [] # list here is to avoid tuple/list type confusion

        for arg in self.args:
            if not isinstance(arg, Type):
                raise TypeError('ZeekEvent constructor requires brokertype arguments')

    def to_broker(self):
        return {
            '@data-type': 'vector',
            'data': [
                {
                    "@data-type": "count",
                    "data": 1
                },
                {
                    "@data-type": "count",
                    "data": 1
                },
                {
                    "@data-type": "vector",
                    "data": [
                        String(self.name).to_broker(),
                        {
                            "@data-type": "vector",
                            "data": [arg.to_broker() for arg in self.args],
                        },
                    ],
                },
            ],
        }

    @classmethod
    def from_vector(cls, vec):
        """Special case for an existing Vector instance: recast as Zeek event."""
        if not isinstance(vec, Vector):
            raise TypeError('cannot convert non-vector to Zeek event')

        if (not len(vec) == 3 or
            not isinstance(vec[2], Vector) or
            not len(vec[2]) == 2 or
            not isinstance(vec[2][0], String) or
            not isinstance(vec[2][1], Vector)):
            raise TypeError('invalid vector layout for Zeek event')

        name = vec[2][0].to_py()
        args = vec[2][1]
        return ZeekEvent(name, *args._elements)

    @classmethod
    def from_broker(cls, data):
        name = data['data'][2]['data'][0]['data']
        res = ZeekEvent(name)
        for argdata in data['data'][2]['data'][1]['data']:
            res.args.append(from_broker(argdata))
        return res


# ---- Message types ---------------------------------------------------

class MessageType(Type):
    """Base class for Broker messages."""
    @classmethod
    def check_broker_data(cls, data):
        if not isinstance(data, dict):
            raise TypeError('invalid data layout for Broker {}: not an object'
                            .format(cls.__name__))
        if 'type' not in data:
            raise TypeError('invalid data layout for Broker {}: required keys missing'
                            .format(cls.__name__))


class HandshakeMessage(MessageType):
    """The handshake message sent by the client.

    This is just a list of topics to subscribe to. Clients won't receive it.
    """
    def __init__(self, topics=None):
        self.topics = []

        if topics:
            if not isinstance(topics, tuple) and not isinstance(topics, list):
                raise TypeError('HandshakeMessage construction requires a topics list')
            for topic in topics:
                if isinstance(topic, str):
                    self.topics.append(topic)
                    continue
                if isinstance(topic, String):
                    self.topics.append(topic.to_py())
                    continue
                raise TypeError('topics for HandshakeMessage must be Python or brokertype strings')

    def to_broker(self):
        return self.topics

    def to_py(self):
        return self

    @classmethod
    def check_broker_data(cls, data):
        if not isinstance(data, tuple) and not isinstance(data, list):
            raise TypeError('invalid data layout for HandshakeMessage: not an object')

    @classmethod
    def from_broker(cls, data):
        return HandshakeMessage(data)


class HandshakeAckMessage(MessageType):
    """The ACK message returned to the client in response to the handshake.

    Clients won't need to send this.
    """
    def __init__(self, endpoint, version):
        self.endpoint = endpoint
        self.version = version

    def to_broker(self):
        return {
            'type': 'ack',
            'endpoint': self.endpoint,
            'version': self.version,
        }

    def to_py(self):
        return self

    @classmethod
    def check_broker_data(cls, data):
        MessageType.check_broker_data(data)
        for key in ('type', 'endpoint', 'version'):
            if key not in data:
                raise TypeError('invalid data layout for HandshakeAckMessage: '
                                'required key "{}" missing'.format(key))

    @classmethod
    def from_broker(cls, data):
        return HandshakeAckMessage(data['endpoint'], data['version'])


class DataMessage(MessageType):
    def __init__(self, topic, data):
        self.topic = topic
        self.data = data

    def to_broker(self):
        bdata = self.data.to_broker()

        return {
            'type': 'data-message',
            'topic': self.topic,
            '@data-type': bdata['@data-type'],
            'data': bdata['data'],
        }

    def to_py(self):
        return self

    @classmethod
    def check_broker_data(cls, data):
        MessageType.check_broker_data(data)
        for key in ('type', 'topic', '@data-type', 'data'):
            if key not in data:
                raise TypeError('invalid data layout for DataMessage: '
                                'required key "{}" missing'.format(key))

    @classmethod
    def from_broker(cls, data):
        return DataMessage(data['topic'], from_broker({
            '@data-type': data['@data-type'],
            'data': data['data']}))


class ErrorMessage(Type):
    def __init__(self, code, context):
        self.code = code # A string representation of a Broker error code
        self.context = context

    def to_broker(self):
        return {
            'type': 'error',
            'code': self.code,
            'context': self.context,
        }

    def to_py(self):
        return self

    @classmethod
    def check_broker_data(cls, data):
        MessageType.check_broker_data(data)
        for key in ('type', 'code', 'context'):
            if key not in data:
                raise TypeError('invalid data layout for ErrorMessage: '
                                'required key "{}" missing'.format(key))

    @classmethod
    def from_broker(cls, data):
        return ErrorMessage(data['code'], data['context'])


# ---- Factory functions -----------------------------------------------

# This maps the types expressed in Broker's JSON representation to those
# implemented in this module.
_broker_typemap = {
    'none': NoneType,
    'boolean': Boolean,
    'count': Count,
    'integer': Integer,
    'real': Real,
    'string': String,
    'enum-value': Enum,
    'address': Address,
    'port': Port,
    'vector': Vector,
    'set': Set,
    'table': Table,
}

# This maps Broker's message types to ones implemented in this module.  A
# separate map, because Broker expresses the type information differently from
# the above.
_broker_messagemap = {
    'data-message': DataMessage,
    'error': ErrorMessage,
}

def unserialize(data):
    """A factory fnuction that instantiates a brokertype value from Broker wire data.

    This assumes the message content in JSON and first unserializes it into a
    Python data structure. It then calls from_python() to instantiate an object
    of the appropriate class from it.
    """
    try:
        obj = json.loads(data)
    except json.JSONDecodeError as err:
        raise TypeError('cannot parse JSON data: {} -- {}'.format(
            err.msg, data)) from err

    return from_broker(obj)

def from_broker(data):
    """A factory function that turns Python-level data into brokertype instances.

    Consider using unserialize() instead, it starts from raw message data, and
    provides better error handling.

    data: a JSON-unserialized Python data structure.

    Returns: a brokerval instance

    Raises: TypeError in case of invalid input data.
    """
    if not isinstance(data, dict):
        raise TypeError('invalid data layout for Broker data: not an object')

    try:
        typ = _broker_messagemap[data['type']]
        typ.check_broker_data(data)
        return typ.from_broker(data)
    except KeyError:
        pass

    try:
        typ = _broker_typemap[data['@data-type']]
        typ.check_broker_data(data)
        return typ.from_broker(data)
    except KeyError as err:
        raise TypeError('unrecognized Broker type: {}'.format(data)) from err

# Python types we can directly map to ones in this module. This is imperfect by
# design (for example, no non-negative integer type exists that maps to Count),
# but a generic factory adds convenience in some situations. Callers that need
# different mappings need to implement code that converts their data structures
# explicitly.
_python_typemap = {
    type(None): NoneType,
    bool: Boolean,
    dict: Table,
    int: Integer,
    ipaddress.IPv4Address: Address,
    ipaddress.IPv6Address: Address,
    float: Real,
    list: Vector,
    set: Set,
    str: String,
    tuple: Vector,
}

def from_py(data, typ=None, check_none=True):
    """Instantiates a brokertype object from the given Python data.

    Some Python types map naturally to Broker ones, such as bools and strs. For
    those, you can simply provide a value and the function will return the
    appropriate brokertype value. For some types this mapping isn't clear, and
    you need to specify the type explicitly. For composite types like
    sets or dicts the approach applies recursively to their member elements.

    When no type match is found, or the type conversion isn't feasible, this
    raises a TypeError. This can happen for types that don't have an immediate
    equivalent (e.g., Python has no unsigned integers).

    This function currently supports only types constructed from a single
    argument.

    data: a Python-"native" value, such as a str, int, or bool.

    typ (Type): if provided, the function attempts to instantiate an object of
        this type with the given data. By default, the function attempts type
        inference.

    check_none (bool): when True (the default), the function checks whether data
        is None, and shortcuts to returning a NoneType instance.

    Returns: a brokertype instance.

    Raises: TypeError in case problems arise in the type mapping or value
        construction.
    """
    if data is None and check_none:
        return NoneType()

    if typ is not None:
        if not issubclass(typ, Type):
            raise TypeError('not a brokertype: {}'.format(typ.__name__))
    else:
        try:
            typ = _python_typemap[type(data)]
        except KeyError as err:
            raise TypeError('cannot map Python type {} to Broker type'.format(type(data))) from err

    if typ == Table:
        res = Table()
        for key, val in data.items():
            res._elements[from_py(key)] = from_py(val)
        return res

    if typ == Vector:
        res = Vector()
        for elem in data:
            res._elements.append(from_py(elem))
        return res

    if typ == Set:
        res = Set()
        for elem in data:
            res._elements.add(from_py(elem))
        return res

    # For others the constructors of the types in this module should naturally
    # work with the provided value.
    return typ(data)

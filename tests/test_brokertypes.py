#! /usr/bin/env python
"""This verifies the behavior of the types provied by the brokertypes module."""
import os.path
import sys
import unittest

TESTS = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.normpath(os.path.join(TESTS, '..'))

# Prepend the tree's root folder to the module searchpath so we find zeekclient
# via it. This allows tests to run without package installation.
sys.path.insert(0, ROOT)

from zeekclient.brokertypes import *

class TestBrokertypes(unittest.TestCase):
    def assertEqualRoundtrip(self, input):
        # This verifies for the given Brokertype object that it can serialize
        # into Broker's wire format, unserialize, and yield an identical object.
        output = type(input).unserialize(input.serialize())
        self.assertEqual(input, output)

    def assertHash(self, val):
        d = {val: 1}
        self.assertEqual(d[val], 1)

    def test_none(self):
        self.assertEqual(NoneType(None), NoneType())
        self.assertEqual(NoneType().to_py(), None)
        self.assertEqual(NoneType(), from_py(None))

        self.assertNotEqual(NoneType, None)

        self.assertEqualRoundtrip(NoneType())

        self.assertFalse(NoneType() < NoneType())
        self.assertHash(NoneType())

    def test_boolean(self):
        self.assertEqual(Boolean(True), Boolean(True))
        self.assertEqual(Boolean(True), Boolean('true'))
        self.assertEqual(Boolean(True).to_py(), True)
        self.assertEqual(Boolean(True), from_py(True))

        self.assertNotEqual(Boolean(True), Boolean(False))
        self.assertNotEqual(Boolean(True), True)

        self.assertEqualRoundtrip(Boolean(True))
        self.assertEqualRoundtrip(Boolean(False))

        self.assertTrue(Boolean(False) < Boolean(True))
        self.assertFalse(Boolean(False) > Boolean(True))
        self.assertHash(Boolean(True))

    def test_count(self):
        self.assertEqual(Count(10), Count(10))
        self.assertEqual(Count(10), Count('10'))
        self.assertEqual(Count(10).to_py(), 10)
        self.assertEqual(Count(10), from_py(10, Count))

        self.assertNotEqual(Count(10), Count(1))
        self.assertNotEqual(Count(10), 10)

        self.assertEqualRoundtrip(Count(10))

        for val in (-10, 'hello'):
            with self.assertRaises(ValueError):
                Count(val)

        self.assertTrue(Count(1) < Count(10))
        self.assertTrue(Count(10) > Count(1))
        self.assertHash(Count(10))

    def test_integer(self):
        self.assertEqual(Integer(10), Integer(10))
        self.assertEqual(Integer(10), Integer('10'))
        self.assertEqual(Integer(10).to_py(), 10)
        self.assertEqual(Integer(10), from_py(10))

        self.assertNotEqual(Integer(10), Integer(1))
        self.assertNotEqual(Integer(10), 10)

        self.assertEqualRoundtrip(Integer(10))

        self.assertTrue(Integer(1) < Integer(10))
        self.assertTrue(Integer(10) > Integer(1))
        self.assertHash(Integer(10))

    def test_real(self):
        self.assertEqual(Real(10.1), Real(10.1))
        self.assertEqual(Real(10.1), Real('10.1'))
        self.assertEqual(Real(10.1).to_py(), 10.1)
        self.assertEqual(Real(10.1), from_py(10.1))

        self.assertNotEqual(Real(10.0), Real(10.1))
        self.assertNotEqual(Real(1.1), 1.1)

        self.assertEqualRoundtrip(Real(1.1))

        with self.assertRaises(ValueError):
            Real('hello')

        self.assertTrue(Real(1) < Real(10))
        self.assertTrue(Real(10) > Real(1))
        self.assertHash(Real(10.1))

    def test_timespan(self):
        self.assertEqual(Timespan(10, Timespan.Unit.NS), Timespan(10, Timespan.Unit.NS))
        self.assertEqual(Timespan(10, Timespan.Unit.NS).to_py(),
                         Timespan(10, Timespan.Unit.NS)) # No native Python representation

        self.assertNotEqual(Timespan(10, Timespan.Unit.NS), Timespan(20, Timespan.Unit.NS))
        self.assertNotEqual(Timespan(10, Timespan.Unit.NS), Timespan(10, Timespan.Unit.S))
        self.assertNotEqual(Timespan(10, Timespan.Unit.NS), 10)

        self.assertEqualRoundtrip(Timespan(10, Timespan.Unit.H))

        with self.assertRaises(ValueError):
            Timespan('oops', Timespan.Unit.NS)
        with self.assertRaises(TypeError):
            Timespan(10, 'ns')

        self.assertTrue(Timespan(10, Timespan.Unit.NS) < Timespan(10, Timespan.Unit.MS))
        self.assertTrue(Timespan(10, Timespan.Unit.MS) > Timespan(10, Timespan.Unit.NS))
        self.assertTrue(Timespan(10, Timespan.Unit.MS) < Timespan(10, Timespan.Unit.S))
        self.assertTrue(Timespan(10, Timespan.Unit.S) < Timespan(10, Timespan.Unit.MIN))
        self.assertTrue(Timespan(10, Timespan.Unit.MIN) < Timespan(10, Timespan.Unit.H))
        self.assertTrue(Timespan(10, Timespan.Unit.H) < Timespan(10, Timespan.Unit.D))
        self.assertHash(Timespan(10, Timespan.Unit.S))

    def test_string(self):
        self.assertEqual(String('10'), String('10'))
        self.assertEqual(String(True), String(True))
        self.assertEqual(String('23'), String(23))
        self.assertEqual(String('23').to_py(), '23')
        self.assertEqual(String('foo'), from_py('foo'))

        self.assertNotEqual(String('10'), String('20'))

        self.assertEqualRoundtrip(String('foo'))

        self.assertTrue(String('bar') < String('foo'))
        self.assertTrue(String('foo') > String('bar'))
        self.assertHash(String('foo'))

    def test_enum(self):
        self.assertEqual(Enum('Foo'), Enum('Foo'))
        self.assertEqual(Enum('Foo').to_py(), 'Foo')

        self.assertNotEqual(Enum('Foo'), Enum('FOO'))
        self.assertNotEqual(Enum('Foo'), Enum('Bar'))

        self.assertEqualRoundtrip(Enum('Foo::bar'))

        self.assertTrue(Enum('FOO::bar') < Enum('FOO::baz'))
        self.assertTrue(Enum('FOO::baz') > Enum('FOO::bar'))
        self.assertHash(Enum('Foo::bar'))

    def test_address(self):
        self.assertEqual(Address('127.0.0.1'), Address('127.0.0.1'))
        self.assertEqual(Address(ipaddress.ip_address('127.0.0.1')), Address('127.0.0.1'))
        self.assertEqual(Address(ipaddress.ip_address('2001:db8::')), Address('2001:db8::'))
        self.assertEqual(Address('127.0.0.1').to_py(), '127.0.0.1')
        self.assertEqual(Address('127.0.0.1'), from_py(ipaddress.ip_address('127.0.0.1')))

        self.assertNotEqual(Address('127.0.0.1'), Address('10.0.0.1'))

        self.assertEqualRoundtrip(Address('10.0.0.1'))

        for val in ('foo', '10.0.0.0/8'):
            with self.assertRaises(ValueError):
                Address(val)

        self.assertTrue(Address('1.0.0.1') < Address('1.0.0.2'))
        self.assertTrue(Address('1.0.0.2') > Address('1.0.0.1'))
        self.assertHash(Address('127.0.0.1'))

    def test_port(self):
        self.assertEqual(Port(10), Port(10))
        self.assertEqual(Port(10), Port('10'))
        self.assertEqual(Port(10, Port.Proto.TCP), Port(10, Port.Proto.TCP))
        self.assertEqual(Port(10, Port.Proto.UDP), Port(10, Port.Proto.UDP))
        self.assertEqual(Port(10, Port.Proto.ICMP), Port(10, Port.Proto.ICMP))
        self.assertEqual(Port(10, Port.Proto.UNKNOWN), Port(10, Port.Proto.UNKNOWN))
        self.assertEqual(Port(10).to_py(), Port(10))

        self.assertEqualRoundtrip(Port(443))
        self.assertEqualRoundtrip(Port(53, Port.Proto.UDP))

        self.assertNotEqual(Port(10), Port(20))
        self.assertNotEqual(Port(10), Port(10, Port.Proto.UDP))
        self.assertNotEqual(Port(10, Port.Proto.UDP), Port(10, Port.Proto.ICMP))
        self.assertNotEqual(Port(10, Port.Proto.UDP), Port(10, Port.Proto.UNKNOWN))

        with self.assertRaises(ValueError):
            Port('oops', Port.Proto.TCP)
        with self.assertRaises(ValueError):
            Port(70000)
        with self.assertRaises(TypeError):
            Port(10, 'tcp')

        self.assertTrue(Port(10) < Port(20))
        self.assertTrue(Port(20) > Port(10))
        self.assertTrue(Port(20, Port.Proto.TCP) < Port(10, Port.Proto.UDP))
        self.assertTrue(Port(20, Port.Proto.UDP) < Port(10, Port.Proto.ICMP))
        self.assertHash(Port(10))

    def test_vector(self):
        val = Vector([from_py(1), from_py('foo'), from_py(True)])

        self.assertEqual(val, val)
        self.assertEqual(Vector([String('foo')]).to_py(), ['foo'])
        self.assertEqual(Vector([String('foo')]), from_py(['foo']))

        self.assertNotEqual(Vector([from_py(1), from_py('foo')]),
                            Vector([from_py(1)]))
        self.assertNotEqual(Vector([from_py(1), from_py('foo')]),
                            Vector([from_py(1), from_py('noo')]))

        self.assertEqualRoundtrip(Vector([from_py(1), from_py('foo'), from_py(True)]))

        for _ in val:
            pass
        self.assertEqual(len(val), 3)
        self.assertEqual(val[0], Integer(1))

        self.assertTrue(Vector([from_py(1)]) < Vector([from_py(2)]))
        self.assertTrue(Vector([from_py(1)]) < Vector([from_py(1), from_py('foo')]))
        self.assertHash(val)

        for val in (23, [23]):
            with self.assertRaises(TypeError):
                Vector(val)

    def test_set(self):
        val = Set({from_py(1), from_py('foo'), from_py(True)})

        self.assertEqual(val, val)
        self.assertEqual(Set({String('foo')}).to_py(), {'foo'})
        self.assertEqual(Set({String('foo')}), from_py({'foo'}))

        self.assertNotEqual(Set({from_py(1), from_py('foo')}),
                            Set({from_py(1)}))
        self.assertNotEqual(Set({from_py(1), from_py('foo')}),
                            Set({from_py(1), from_py('noo')}))

        self.assertEqualRoundtrip(Set({from_py(1), from_py('foo'), from_py(True)}))

        for _ in val:
            pass
        self.assertEqual(len(val), 3)
        self.assertTrue(Integer(1) in val)

        self.assertTrue(Set({from_py(1)}) < Set({from_py(2)}))
        self.assertTrue(Set({from_py(1)}) < Set({from_py(1), from_py('foo')}))
        self.assertHash(val)

        for val in (23, {23}):
            with self.assertRaises(TypeError):
                Set(val)

    def test_table(self):
        val = Table({from_py('foo'): from_py(1),
                     from_py('bar'): from_py(2)})

        self.assertEqual(val, val)
        self.assertEqual(Table({from_py('foo'): from_py(1),
                                from_py('bar'): from_py(2)}).to_py(),
                         {'foo': 1, 'bar': 2})
        self.assertEqual(Table({from_py('foo'): from_py(1)}),
                         from_py({'foo': 1}))

        self.assertNotEqual(Table({from_py('foo'): from_py(1),
                                   from_py('bar'): from_py(2)}),
                            Table({from_py('foo'): from_py(1),
                                   from_py('bar'): from_py(3)}))
        self.assertNotEqual(Table({from_py('foo'): from_py(1),
                                   from_py('bar'): from_py(2)}),
                            Table({from_py('foo'): from_py(1),
                                   from_py('baz'): from_py(2)}))

        self.assertEqualRoundtrip(Table({from_py('foo'): from_py(1),
                                         from_py('bar'): from_py(2)}))
        for _ in val:
            pass
        for _ in val.keys():
            pass
        for _ in val.values():
            pass
        for _, _ in val.items():
            pass
        self.assertEqual(len(val), 2)
        self.assertTrue(String('foo') in val)

        self.assertFalse(Table({from_py('foo'): from_py(1)}) <
                         Table({from_py('foo'): from_py(1)}))
        self.assertTrue(Table({from_py('foo'): from_py(1)}) <
                        Table({from_py('foo'): from_py(2)}))
        self.assertTrue(Table({from_py('bar'): from_py(1)}) <
                        Table({from_py('foo'): from_py(1),
                               from_py('bar'): from_py(1)}))
        self.assertTrue(Table({from_py('foo'): from_py(1)}) <
                        Table({from_py('foo'): from_py(1),
                               from_py('bar'): from_py(2)}))
        self.assertTrue(Table({from_py('aaa'): from_py(1)}) <
                        Table({from_py('foo'): from_py(1),
                               from_py('bar'): from_py(2)}))
        self.assertHash(val)

        for val in (23, {'foo': 23}):
            with self.assertRaises(TypeError):
                Table(val)

    def test_zeek_event(self):
        evt = ZeekEvent('Test::event', from_py('hello'), from_py(42), from_py(True))
        self.assertTrue(isinstance(evt, Vector))
        self.assertEqual(ZeekEvent('Test::event', from_py('hello'), from_py(42), from_py(True)),
                         ZeekEvent('Test::event', from_py('hello'), from_py(42), from_py(True)))
        self.assertNotEqual(ZeekEvent('Test::event', from_py('hello'), from_py(42), from_py(True)),
                            ZeekEvent('Test::event2', from_py('hello'), from_py(42), from_py(True)))
        self.assertNotEqual(ZeekEvent('Test::event', from_py('hello'), from_py(42), from_py(True)),
                            ZeekEvent('Test::event', from_py('hello'), from_py(42)))
        self.assertNotEqual(ZeekEvent('Test::event', from_py('hello'), from_py(43)),
                            ZeekEvent('Test::event', from_py('hello'), from_py(42)))

        self.assertEqualRoundtrip(evt)

        vec = Vector.unserialize(evt.serialize())
        evt2 = ZeekEvent.from_vector(vec)
        self.assertEqual(evt, evt2)

        with self.assertRaises(TypeError):
            ZeekEvent.from_vector(String('not a vector'))

        with self.assertRaises(TypeError):
            ZeekEvent('foo', 1)

    def test_handshake_message(self):
        self.assertEqual(HandshakeMessage(['foo', 'bar']),
                         HandshakeMessage(['foo', String('bar')]))
        self.assertNotEqual(HandshakeMessage(['foo', 'bar']),
                            HandshakeMessage(['foo']))
        self.assertEqualRoundtrip(HandshakeMessage(['foo', 'bar']))

    def test_handshake_ack_message(self):
        self.assertEqual(HandshakeAckMessage('aaaa', '1.0'),
                         HandshakeAckMessage('aaaa', '1.0'))
        self.assertNotEqual(HandshakeAckMessage('aaaa', '1.0'),
                            HandshakeAckMessage('bbbb', '1.0'))
        self.assertEqualRoundtrip(HandshakeAckMessage('aaaa', '1.0'))

    def test_data_message(self):
        self.assertEqual(DataMessage('foo', String('test')),
                         DataMessage('foo', String('test')))
        self.assertNotEqual(DataMessage('foo', String('test')),
                            DataMessage('foo', String('other')))
        self.assertEqualRoundtrip(DataMessage('foo', String('test')))

    def test_error_message(self):
        msg1 = ErrorMessage('deserialization_failed', 'this is where you failed')
        msg2 = ErrorMessage('deserialization_failed', 'this is where you also failed')
        self.assertEqual(msg1, msg1)
        self.assertNotEqual(msg1, msg2)
        self.assertEqualRoundtrip(msg1)

    def test_type_lt(self):
        # Any brokertyped data value can be compared to any other, but not to
        # unrelated types.
        self.assertTrue(Boolean(True) < Count(1))
        self.assertTrue(Count(1) < Integer(1))
        self.assertTrue(Integer(1) < Real(1))
        self.assertTrue(Real(1) < Set())
        self.assertTrue(Set() < String('foo'))
        self.assertTrue(String('foo') < Timespan(10, Timespan.Unit.MS))

        with self.assertRaises(TypeError):
            Boolean(True) < True
        with self.assertRaises(TypeError):
            Boolean(True) < HandshakeMessage # Not a data type

        self.assertTrue(Boolean(True) < Count(10))

    def test_unserialize_not_json(self):
        data = b'\x00\x00\x00'

        with self.assertRaisesRegex(TypeError, 'cannot parse JSON data'):
            obj = unserialize(data)
        with self.assertRaisesRegex(TypeError, 'cannot parse JSON data'):
            obj = Count.unserialize(data)

    def test_unserialize_invalid_json(self):
        data = b'[ 1,2,3 ]'
        with self.assertRaisesRegex(TypeError, 'invalid data layout'):
            obj = unserialize(data)
        with self.assertRaisesRegex(TypeError, 'invalid data layout'):
            obj = Count.unserialize(data)

        data = b'{ "data": "foobar" }'
        with self.assertRaisesRegex(TypeError, 'unrecognized Broker type'):
            obj = unserialize(data)
        with self.assertRaisesRegex(TypeError, 'invalid data layout'):
            obj = Count.unserialize(data)

        data = b'{ "data": "foobar", "@data-type": "count" }'
        with self.assertRaisesRegex(TypeError, 'invalid data for Count'):
            obj = Count.unserialize(data)

def test():
    """Entry point for testing this module.

    Returns True if successful, False otherwise.
    """
    res = unittest.main(sys.modules[__name__], verbosity=0, exit=False)
    # This is how unittest.main() implements the exit code itself:
    return res.result.wasSuccessful()

if __name__ == '__main__':
    sys.exit(not test())

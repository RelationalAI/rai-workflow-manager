import unittest

from cli import args


class ArgsTest(unittest.TestCase):

    def test_parse_key_value_1_parameter(self):
        # when
        result = args.parse_string_int_key_value_argument("key=1")
        # then
        self.assertEqual({"key": 1}, result)

    def test_parse_key_value_2_parameters(self):
        # when
        result = args.parse_string_int_key_value_argument("key=1,number=2")
        # then
        self.assertEqual({"key": 1, "number": 2}, result)

    def test_parse_key_value_2_parameters_with_empty_pair(self):
        # when
        result = args.parse_string_int_key_value_argument("key=1,,number=2")
        # then
        self.assertEqual({"key": 1, "number": 2}, result)

    def test_parse_key_value_2_parameters_with_spaces(self):
        # when
        result = args.parse_string_int_key_value_argument("  key  =  1 , num ber =2 ")
        # then
        self.assertEqual({"key": 1, "num ber": 2}, result)

    def test_parse_key_value_prohibited_symbol(self):
        # when
        with self.assertRaises(ValueError):
            args.parse_string_int_key_value_argument("key=1,num=ber=2")
        # then
        # exception

    def test_parse_key_value_invalid_pair(self):
        # when
        with self.assertRaises(ValueError):
            args.parse_string_int_key_value_argument("key=1,b,number=2")
        # then
        # exception

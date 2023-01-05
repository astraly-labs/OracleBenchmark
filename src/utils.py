from starkware.starknet.compiler.compile import get_selector_from_name as starkware_get_selector_from_name

def get_selector_from_name(name):
    return starkware_get_selector_from_name(name)

def str_to_felt(text):
    b_text = bytes(text, "ascii")
    return int.from_bytes(b_text, "big")

def to_uint(a):
    """Takes in value, returns uint256-ish tuple."""
    return (a & ((1 << 128) - 1), a >> 128)

def long_str_to_array(text):
    res = []
    for tok in text:
        res.append(str_to_felt(tok))
    return res

def long_str_to_print_array(text):
    res = []
    for tok in text:
        res.append(str_to_felt(tok))
    return ' '.join(res)

def decimal_to_hex(decimal: int):
    return hex(decimal)

def hex_string_to_decimal(hex_string: str):
    return int(hex_string, 16)

def hex_string_to_string(hex_string):
    hex_string = hex_string[2:]
    return bytes.fromhex(hex_string).decode('utf-8')

def to_unit(value, decimal):
    return value / 10 ** decimal

def get_struct(structs, name):
    return list(filter(lambda x: x['name'] in name, structs))[0]

class DataParser:

    def __init__(self, selector_name, data, members, structs) -> None:
        self.selector_name = selector_name
        self.raw_data = data
        self.data = []
        self.members = members
        self.structs = structs
        self.initialize()

    def initialize(self):
        for index, member in enumerate(self.members):
            member_type = member['type']
            if len(self.members) > index+1 and self.members[index+1]['type'].endswith('*'):
                continue
            if member['type'].endswith('*') and int(self.raw_data[0], 16) == 0:
                value = []
                self.raw_data = self.raw_data[1:]
            elif member['type'] == "felt":
                value = self.raw_data[:1][0]
                self.raw_data = self.raw_data[1:]
            elif member['type'] == "felt*":
                length = int(self.raw_data[0], 16)
                value = self.raw_data[1:length+1]
                self.raw_data = self.raw_data[length+1:]
            elif member['type'].endswith('*'):
                object_length = int(self.raw_data[0], 16)
                self.raw_data = self.raw_data[1:]
                value = [self.build_member_value(member) for i in range(object_length)]
            else:
                value = self.build_member_value(member)
            member_value = {
                "name": member['name'],
                "type": member_type,
                "value": value
            }
            self.data.append(member_value)

    def build_member_value(self, member):
        current_struct = get_struct(self.structs, member['type'])
        struct_val = {}
        for index, struct_member in enumerate(current_struct['members']):
            if len(current_struct['members']) > index+1 and current_struct['members'][index+1]['type'].endswith('*'):
                continue
            elif struct_member['type'].endswith('*') and int(self.raw_data[0], 16) == 0:
                struct_val[struct_member['name']] = []
                self.raw_data = self.raw_data[1:]
            if struct_member['type'] == "felt":
                val = self.raw_data[:1][0]
                self.raw_data = self.raw_data[1:]
                struct_val[struct_member['name']] = val
            elif struct_member['type'] == "felt*":
                length = int(self.raw_data[0], 16)
                val = self.raw_data[1:length+1]
                self.raw_data = self.raw_data[length+1:]
                struct_val[struct_member['name']] = val
            elif struct_member['type'].endswith('*'):
                object_length = int(self.raw_data[0], 16)
                self.raw_data = self.raw_data[1:]
                struct_val[struct_member['name']] = [self.build_member_value(member) for i in range(object_length)]
            else:
                struct_val[struct_member['name']] = self.build_member_value(struct_member)
        return struct_val


def normalize_submit_many_entry(data):
    try:
        price_feed = list(filter(lambda x: x['name'] == "new_entries", data))[0].get('value')
        normalized_feed = [{
            'feed': hex_string_to_string(feed.get('key')),
            'price': hex_string_to_decimal(feed.get('value')),
            'timestamp': hex_string_to_decimal(feed.get('timestamp')),
            'publisher': hex_string_to_string(feed.get('publisher'))
        } for feed in price_feed]
        return normalized_feed
    except:
        return None

def filter_feeds(feed, feed_data):
    return list(filter(lambda f: f['feed'] == feed, feed_data))

def combine_pair(feed_data):
    luna_usd_feed = [entry['price'] for entry in filter_feeds('luna/usd', feed_data)]
    eth_usd_feed = [entry['price'] for entry in filter_feeds('eth/usd', feed_data)]
    return [luna_price / eth_price for luna_price, eth_price in zip(luna_usd_feed, eth_usd_feed)]

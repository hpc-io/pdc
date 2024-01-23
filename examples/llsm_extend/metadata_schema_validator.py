import json
import argparse
from jsonschema import validate
from jsonschema.exceptions import ValidationError

def load_json(file_path):
    """Load JSON data from a file."""
    with open(file_path, 'r') as file:
        return json.load(file)

def validate_json(json_data, json_schema):
    """Validate JSON data against a schema."""
    try:
        validate(instance=json_data, schema=json_schema)
        return "valid"
    except ValidationError as e:
        print(e)
        return "invalid"

def main(schema_path, data_path):
    """Main function to load and validate JSON data against a schema."""
    schema = load_json(schema_path)
    data = load_json(data_path)
    result = validate_json(data, schema)
    print("{} is {}. Object count: {}".format(data_path, result, len(data["objects"])))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate JSON against a schema.")
    parser.add_argument("-s", "--schema", dest="schema_path", required=True, help="Path to the JSON schema file")
    parser.add_argument("-d", "--data", dest="data_path", required=True, help="Path to the JSON data file")
    args = parser.parse_args()
    main(args.schema_path, args.data_path)

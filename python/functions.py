import json

def load_json(file):
    with open(file) as bar:
        try:
            return json.load(bar)
        except json.JSONDecodeError:
            print(f"Failed to load json from {file}")
            return False
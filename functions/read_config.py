import yaml
from datetime import datetime
from dateutil.relativedelta import relativedelta


def read_yaml_config(file_path):
    """
    Reads a YAML configuration file and returns the contents as a dictionary.

    Args:
    file_path (str): Path to the YAML file.

    Returns:
    dict: Contents of the YAML file.
    """
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        d = config.get("time", {})
        t_month = d.get("months")
        start_year = d.get("startyr")
        end_year = d.get("endyr")
        offset = d.get("offset_months", 0)

        # Logic: Use hardcoded values if BOTH are present
        if start_year and end_year:
            print("Using hardcoded date from YAML.")
            return config

        # Fallback: Calculate based on offset
        print(f"Calculating date using offset: -{offset} months.")
        target_date = datetime.now() - relativedelta(months=offset)
        config['time']['months'] = target_date.month
        config['time']['startyr'] = target_date.year
        config['time']['endyr'] = target_date.year

        return config
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' does not exist.")
        return None
    except yaml.YAMLError as exc:
        print(f"Error: Failed to parse YAML file '{file_path}'.")
        print(exc)
        return None

# Example usage
if __name__ == "__main__":
    config_path = 'configs/config.yaml'
    config = read_yaml_config(config_path)

    if config:
        print("Configuration loaded successfully:")
        print(config)
    else:
        print("Failed to load configuration.")
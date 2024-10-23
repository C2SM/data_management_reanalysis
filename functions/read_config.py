import yaml

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
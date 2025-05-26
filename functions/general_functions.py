def convert_month_list(config_month_list):
    """
    Convert comma separated month numbers from config file into list

    Input:
        01, 02, 03,

    Returns:
        list: ["01", "02", "03"]
    """

    try:
        if "," in config_month_list:
            config_month_list = config_month_list.replace('"', "").strip()
            month_list = [item.strip() for item in config_month_list.split(",")]
        else:
            month_list = [f"{config_month_list}"]
    except TypeError:
        month_list = ["{:02d}".format(config_month_list)]

    return month_list


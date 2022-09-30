import configparser


def parse_crawl_cfg() -> dict:
    config = configparser.ConfigParser()
    config.read('crawl.cfg', encoding='utf-8')
    config_dict = {}
    for section_name in config._sections.keys():
        ordered_dict = config._sections[section_name]
        section_dict = {}
        for key in ordered_dict.keys():
            value = ordered_dict[key]
            if section_name == 'thread' and key == 'thread_num':
                value = int(value)
            section_dict[f"{key}"] = value
        config_dict[section_name] = section_dict
    return config_dict


config_dict = parse_crawl_cfg()
__all__ = ['config_dict']

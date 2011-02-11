import ConfigParser

class Unspecified:
    pass

class Config(ConfigParser.SafeConfigParser):
    def get(self, section, value, default = Unspecified):
        try:
            return ConfigParser.SafeConfigParser.get(self, section, value)
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            if default is Unspecified:
                raise
            return default


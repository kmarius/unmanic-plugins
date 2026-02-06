import os

from unmanic.libs.unplugins.settings import PluginSettings

from kmarius_cache_metadata.lib.metadata_provider import PROVIDERS
from kmarius_cache_metadata.lib.plugin_types import *
from kmarius_cache_metadata.lib import logger, cache

cache.init([provider.name for provider in PROVIDERS])


class Settings(PluginSettings):
    @staticmethod
    def __build_settings():
        settings = {
            "quiet_caching": False,
        }
        form_settings = {
            "quiet_caching": {
                'label': "Don't log cache lookups and updates.",
            }
        }

        settings.update({
            f"enable_{provider.name}_caching": provider.default_enabled for provider in PROVIDERS
        })

        form_settings.update({
            f"enable_{p.name}_caching": {
                'label': f'Enable {p.name} metadata caching',
            } for p in PROVIDERS
        })

        return settings, form_settings

    def __init__(self, *args, **kwargs):
        super(Settings, self).__init__(*args, **kwargs)
        self.settings, self.form_settings = self.__build_settings()


def on_library_management_file_test(data: FileTestData):
    settings = Settings(library_id=data["library_id"])

    path = data["path"]
    mtime = int(os.path.getmtime(path))
    quiet = settings.get_setting("quiet_caching")

    for provider in PROVIDERS:
        if not settings.get_setting(f"enable_{provider.name}_caching"):
            continue

        res = cache.get(provider.name, path, mtime, reuse_connection=True)

        if res:
            if not quiet:
                logger.info(f"Cached {provider.name} data found - {path}")
        else:
            if not quiet:
                logger.info(f"No cached {provider.name} data found, refreshing - {path}")
            res = provider.run_prog(path)
            if res:
                cache.put(provider.name, path, mtime, res, reuse_connection=True)
            else:
                if not quiet:
                    logger.error(f"Could not retrieve {provider.name} metadata - {path}")

        if res:
            data["shared_info"][provider.name] = res
# -*- coding: utf-8 -*-

class Config(object):
    def __init__(self, module_name = None, **kwargs):
        self._options = {}

        def items():
            """Генератор последовательности настроек.

            Сначала настройки берутся из указанного модуля, потом те,
            что заданы явно при создании конфига.
            """
            if module_name is not None:
                module = __import__(module_name, fromlist=[module_name])
                for item in module.__dict__.iteritems():
                    yield item
            for item in kwargs.iteritems():
                yield item

        self._options.update(
            (key, value)
            for key, value in items()
            if key.upper() == key
        )

    def __getattr__(self, name):
        return self._options.get(name)


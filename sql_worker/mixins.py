from abc import ABCMeta, abstractmethod


class MakeQueryMixin(metaclass=ABCMeta):

    @abstractmethod
    def _value_format(self, value):
        raise NotImplementedError

    def _make_data(self, **kwargs):
        data = ''
        kwargs_len = len(kwargs)

        for i, key in enumerate(kwargs):
            value = self._value_format(kwargs[key])
            data += f'{key}={value}'

            if i != kwargs_len - 1:
                data += ', '
        return data

    def _make_where(self, **kwargs):
        where = 'WHERE ' if kwargs else ''
        kwargs_len = len(kwargs)

        for i, key in enumerate(kwargs):
            value = self._value_format(kwargs[key])
            where += f'{key}={value}'

            if i != kwargs_len - 1:
                where += ' and '
        return where

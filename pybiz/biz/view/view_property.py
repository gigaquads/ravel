from typing import Text, Type, Tuple

from pybiz.util import is_sequence
from pybiz.util.loggers import console


class ViewProperty(property):
    def __init__(self, view, **kwargs):
        super().__init__(**kwargs)
        self._view = view

    def __repr__(self):
        return f'<ViewProperty(name={self._view.name})>'

    @classmethod
    def build(cls, view: 'View') -> 'ViewProperty':
        """
        """
        def fget(self):
            """
            """
            # "self" refers to the BizObject instance that has the view
            value = None

            if view.name not in self._viewed:
                console.debug(
                    message=f'lazy loading ViewProperty {view.name}',
                    data={'object': str(self), 'view': str(view)}
                )
                value = view.query(self)
                setattr(self, view.name, value)
            else:
                value = self._viewed.get(view.name)

            return value

        def fset(self, value):
            """
            """
            # "self" refers to the BizObject instance that has the view
            if view.schema is not None:
                data, errors = view.schema.process(value)
                if errors:
                    # TODO: raise proper exception
                    console.error(
                        message=(
                            f'validation error in setting '
                            f'ViewProperty {view.name}',
                        ),
                        data={
                            'object': str(self),
                            'errors': errors,
                        }
                    )
                    raise Exception('validation error')
                self._viewed[view.name] = data
            else:
                self._viewed[view.name] = value

        def fdel(self):
            """
            """
            # "self" refers to the BizObject instance that has the view
            self._viewed.pop(view.name, None)

        return cls(view=view, fget=fget, fset=fset, fdel=fdel)

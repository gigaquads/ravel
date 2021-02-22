import traceback

from threading import current_thread

from ravel.util.misc_functions import get_class_name


class TransactionManager:
    def __init__(self, app: 'Application'):
        self.log = app.log
        self.store_classes = set(
            b.store_class for b in app.manifest.bindings
        )

    def __repr__(self):
        return (
            f'{get_class_name(self)}(thread={current_thread.name})'
        )

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is None:
            self.commit()
        else:
            self.log.error(
                message=f'aborting transaction due to {exc_value}',
                data={'traceback': traceback.format_exc().split('\n')}
            )
            self.rollback()

    def begin(self, **kwargs):
        for store_class in self.store_classes:
            try:
                store_class.begin(**kwargs)
            except Exception as exc:
                self.log.error(
                    message=f'failed to create transaction',
                    data={'store': store_class}
                )
                raise

    def commit(self, **kwargs):
        for store_class in self.store_classes:
            try:
                exc = None
                if store_class.has_transaction():
                    store_class.commit(**kwargs)
            except Exception as exc:
                self.log.error(
                    message=f'failed to commit transaction. rolling back',
                    data={'store': store_class}
                )
            if exc:
                try:
                    store_class.rollback()
                except Exception:
                    self.log.exception(
                        message=f'failed to rollback transaction',
                        data={'store': store_class}
                    )
        if exc:
            raise Exception('a transaction failed')

    def rollback(self, **kwargs):
        for store_class in self.store_classes:
            try:
                if store_class.has_transaction():
                    store_class.rollback(**kwargs)
            except Exception as exc:
                self.log.error(
                    message=f'failed to rollback transaction',
                    data={'store': store_class}
                )
                raise

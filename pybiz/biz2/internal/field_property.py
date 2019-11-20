class FieldProperty(property):
    def __init__(self, biz_class, field):
        self.biz_class = biz_class
        self.field = field
        super().__init__(
            fget=self.on_get,
            fset=self.on_set,
            fdel=self.on_delete
        )

    def on_set(self, biz_obj, value):
        processed_value, error = self.field.process(value)
        biz_obj.internal.data[self.field.name] = processed_value

    def on_get(self, biz_obj):
        return biz_obj.internal.data.get(self.field.name)

    def on_delete(self, biz_obj):
        del biz_obj.internal.data[self.field.name]

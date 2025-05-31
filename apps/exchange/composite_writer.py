# apps/exchange/composite_writer.py
class CompositeWriter:
    def __init__(self, *writers):
        self.writers = writers
        required = {"record_order","record_trade","record_cancel",
                    "list_instruments","iter_orders","create_instrument"}
        for w in writers:
            missing = required - set(dir(w))
            if missing:
                raise AttributeError(f"{w} missing {missing}")

    def __getattr__(self, name):
        def _wrapper(*args, **kwargs):
            # 1) Call the method on the first writer and capture its return value
            result = getattr(self.writers[0], name)(*args, **kwargs)
            # 2) Fan out to any additional writers (ignore their return values)
            for w in self.writers[1:]:
                getattr(w, name)(*args, **kwargs)
            # 3) Return the result from the first writer
            return result
        return _wrapper
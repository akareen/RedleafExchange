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
            result = getattr(self.writers[0], name)(*args, **kwargs)
            for w in self.writers[1:]:
                getattr(w, name)(*args, **kwargs)
            return result
        return _wrapper
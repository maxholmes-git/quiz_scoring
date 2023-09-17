from importlib import import_module
import pkgutil
import inspect
import sys


for importer, modname, ispkg in pkgutil.iter_modules(["validate_inputs"]):
    imported_module = import_module(f"validate_inputs.{modname}")
    if modname in dir(imported_module):
        attribute = getattr(imported_module, modname)
        if inspect.isfunction(attribute):
            setattr(sys.modules[__name__], modname, attribute)

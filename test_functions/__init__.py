from importlib import import_module
import pkgutil
import inspect
import sys


for importer, modname, ispkg in pkgutil.iter_modules(["test_functions"]):
    imported_module = import_module(f"test_functions.{modname}")
    if modname in dir(imported_module):
        attribute = getattr(imported_module, modname)
        if inspect.isfunction(attribute):
            setattr(sys.modules[__name__], modname, attribute)

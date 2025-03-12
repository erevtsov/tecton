def load_reference(
    module: str,
    object: str = None,
    function: str = None,
    constructor_params: dict = {},
) -> object:
    """
    Dynamically return reference to entity based on module/object/function inputs

    :param module: Full qualified module name

    :param object: Optional name of object to instantiate
    :param function: Optional name of function to return
    :param constructor_params: Optional arguments to pass into object constructor

    :return: Reference to module/object/function, depending on input parameters
    """
    import importlib

    # if only module is specified, import it and return reference
    mod = importlib.import_module(module)
    if (not object) and (not function):
        return mod
    # if function is specified but no object, assume function is definied directly in module
    if function and (not object):
        func = getattr(mod, function)
        return func

    # otherwise try to instantiate the object and then get the function reference
    klass = getattr(mod, object)
    obj = klass(**constructor_params)
    if function:
        func = getattr(obj, function)
        return func
    else:
        return obj

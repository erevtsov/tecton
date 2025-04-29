import yaml


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


class TableSet:
    """
    A nested structure of TableConfig objects
    """

    def __init__(self, data: dict):
        for key, value in data.items():
            if isinstance(value, dict):
                # If this child has a 'path', it's a TableConfig (leaf)
                if 'path' in value:
                    setattr(self, key, TableConfig(value))
                else:
                    setattr(self, key, TableSet(value))
            else:
                setattr(self, key, value)

    def __str__(self):
        def tree_view(obj, indent=''):
            result = []
            for key, value in obj.__dict__.items():
                if isinstance(value, TableSet):
                    result.append(f'{indent}{key}')
                    result.append(tree_view(value, indent + '  '))
                elif isinstance(value, TableConfig):
                    result.append(f'{indent}{key}')
                else:
                    result.append(f'{indent}{key}: {value}')
            return '\n'.join(result)

        return tree_view(self)

    __repr__ = __str__


class TableConfig:
    """
    A configuration object for a table
    """

    def __init__(self, data: dict):
        for key, value in data.items():
            setattr(self, key, value)

    def __str__(self):
        return str(getattr(self, 'path', ''))

    __repr__ = __str__


def load_yaml_tables(file_path: str) -> TableSet:
    """
    Load a YAML file containing table configurations and return a TableSet object.
    """
    with open(file_path) as f:
        data = yaml.safe_load(f)
    return TableSet(data)

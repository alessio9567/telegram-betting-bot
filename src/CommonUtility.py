import yaml
import os

REQUIREMENTS_PATH = os.path.dirname(os.getcwd())

def getParameterFromFile(parameterName):
    with open(r'{}/requirements.yaml'.format(REQUIREMENTS_PATH)) as file:
        parameter_dict=yaml.load(file, Loader=yaml.FullLoader)
        for key in parameter_dict:
            if key == parameterName:
                return parameter_dict[key]
        return ''

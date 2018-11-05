# This import line below allows pacsman to be used as a submodule without double naming on imports.
# For example:
#     pacsman.pacsman import BaseDicomClient
# Simplifies to:
#     from pacsman import BaseDicomClient
# More importantly imports do not need to change if later pacsman is installed as a package instead.
from .pacsman import *

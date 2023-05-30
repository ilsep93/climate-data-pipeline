import pkg_resources

from utils import read_db_table, session

installed = {pkg.key for pkg in pkg_resources.working_set}
__all__ = ["read_db_table", "session"]
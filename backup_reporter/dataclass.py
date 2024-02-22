import json

from dataclasses import dataclass, asdict


@dataclass
class BackupMetadata:
    '''Class contain fields with info about backup'''
    type: str = None
    size: str = None
    time: str = None
    customer: str = None
    placement: str = None
    backup_name: str = None
    description: str = None
    last_backup_date: str = None
    count_of_backups: str = None
    supposed_backups_count: str = None

    def __str__(self):
        '''String representation of that DataClass is valid json string'''
        return json.dumps(asdict(self))

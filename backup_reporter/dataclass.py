import json
import datetime

from dataclasses import dataclass, asdict, field


@dataclass
class BackupFileInfo:
    size: str = None
    backup_date: str = None
    backup_name: str = None
    sha1sum: str = None
    def __str__(self):
        '''String representation of that DataClass is valid json string'''
        return json.dumps(asdict(self), default=str)


@dataclass
class BackupMetadata:
    '''Class contain fields with info about backup'''
    type: str = None
    format: str = "json"
    size: str = None
    time: str = None
    customer: str = None
    placement: str = None
    backup_name: str = None
    description: str = None
    last_backup_date: str = None
    count_of_backups: str = None
    supposed_backups_count: str = None
    sha1sum: str = None
    backups: list[BackupFileInfo] = field(default_factory=list)

    def __str__(self):
        '''String representation of that DataClass is valid json string'''
        if self.format == "json":
            return json.dumps(asdict(self), default=str)
        elif self.format == "prom":
            return self.__get_prom_format()
        else:
            print(f"Got {self.format}, need json or prom")

    def __get_prom_format(self):
        prom_lines = []

        base_labels = [f'customer="{self.customer}"']
        if self.type:
            base_labels.append(f'type="{self.type}"')
        if self.placement:
            base_labels.append(f'placement="{self.placement}"')
        if self.description:
            base_labels.append(f'br_description="{self.description}"')

        base_label_str = ",".join(base_labels)

        if self.count_of_backups is not None:
            prom_lines.append("# HELP backup_count Number of backups found")
            prom_lines.append("# TYPE backup_count gauge")
            try:
                prom_lines.append(f'backup_count{{{base_label_str}}} {int(self.count_of_backups)}')
            except ValueError as exc:
                prom_lines.append(f'backup_count{{{base_label_str}}} {int(self.count_of_backups.split(" ")[0])}')

        if self.supposed_backups_count is not None:
            prom_lines.append("# HELP supposed_backups_count Expected number of backups")
            prom_lines.append("# TYPE supposed_backups_count gauge")
            prom_lines.append(f'supposed_backups_count{{{base_label_str}}} {int(self.supposed_backups_count)}')

        prom_lines.append("# HELP last_backup_size_mb Size of last backup in MB")
        prom_lines.append("# TYPE last_backup_size_mb gauge")
        prom_lines.append(f'last_backup_size_mb{{{base_label_str},backup_name="{self.backup_name}"}} {float(self.size)}')

        if self.time:
            duration_seconds = self._parse_duration_to_seconds(self.time)
            prom_lines.append("# HELP backup_duration_seconds Duration of last backup in seconds")
            prom_lines.append("# TYPE backup_duration_seconds gauge")
            prom_lines.append(f'last_backup_duration_seconds{{{base_label_str},backup_name="{self.backup_name}"}} {duration_seconds}')

        if self.last_backup_date:
            dt = datetime.datetime.fromisoformat(str(self.last_backup_date).replace("Z","+00:00"))
            prom_lines.append("# HELP last_backup_timestamp Unix timestamp of last backup file")
            prom_lines.append("# TYPE last_backup_timestamp gauge")
            prom_lines.append(f'last_backup_timestamp{{{base_label_str},backup_name="{self.backup_name}"}} {int(dt.timestamp())}')

        if self.backups:
            prom_lines.append("# HELP backup_file_size_mb Size of backup file in MB")
            prom_lines.append("# TYPE backup_file_size_mb gauge")
            prom_lines.append("# HELP backup_file_date_timestamp Unix timestamp of backup file")
            prom_lines.append("# TYPE backup_file_date_timestamp gauge")

            for backup in self.backups:
                safe_name = backup.backup_name.replace('"', '\\"')
                file_labels = base_label_str + f',backup_name="{safe_name}"'
                if backup.sha1sum:
                    file_labels += f',sha1sum="{backup.sha1sum}"'

                if backup.size is not None:
                    prom_lines.append(f'backup_file_size_mb{{{file_labels}}} {float(backup.size)}')

                if backup.backup_date:
                    try:
                        dt = datetime.datetime.fromisoformat(str(backup.backup_date).replace("Z","+00:00"))
                        prom_lines.append(f'backup_file_date_timestamp{{{file_labels}}} {int(dt.timestamp())}')
                    except Exception:
                        pass

        return "\n".join(prom_lines) + "\n"

    def _parse_duration_to_seconds(self, duration_str):
        parts = duration_str.split(':')
        seconds = 0.0
        if len(parts) == 3:
            hours = float(parts[0])
            minutes = float(parts[1])
            seconds_part = parts[2]
            seconds = hours * 3600 + minutes * 60 + float(seconds_part)
        elif len(parts) == 2:
            minutes = float(parts[0])
            seconds_part = parts[1]
            seconds = minutes * 60 + float(seconds_part)
        else:
            seconds = float(parts[0])
        return seconds

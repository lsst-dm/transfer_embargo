# This file is part of transfer_embargo
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__all__ = ["DataQuery"]

from typing import Any, Optional, Self

import pydantic
import yaml


class DataQuery(pydantic.BaseModel):
    collections: str | list[str]
    """Collection names or glob patterns to search."""

    dataset_types: str | list[str]
    """List of dataset types or glob patterns to transfer."""

    instrument: str
    """Instrument this query pertains to."""

    where: str
    """Where clause expression to select datasets to transfer."""

    embargo_hours: float
    """How long to embargo the selected datasets (hours)."""

    avoid_dstypes_from_collections: Optional[str | list[str]] = None
    """Collections containing dataset types to avoid transferring."""

    @classmethod
    def from_yaml(cls, yaml_source: Any) -> list[Self]:
        result = []
        for entry in yaml.safe_load(yaml_source):
            result.append(cls(**entry))
        return result

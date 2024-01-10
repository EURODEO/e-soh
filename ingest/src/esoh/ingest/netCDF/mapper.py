import json
import os


class mapper():
    """
    Delivers and loads json mappings for netCDF metadata parsing
    Will lazy load maps.
    """

    def __init__(self, schema_path):
        self.schema_path = schema_path
        with open(os.path.join(self.schema_path, "institution_mapping.json"), "r") as file:
            self.path_to_maps = json.load(file)

        self.maps = {}

    def _verify_known_institution(self, institution):
        if institution not in self.path_to_maps:
            raise ValueError(f"There are no defined mapping for the institution {institution}")

    def __call__(self, institution: str) -> dict:
        """
        Call an instance of this class for the correct json mapping given the institution name.
        Must be same name as the global field from netCDF.
        """
        self._verify_known_institution(institution)
        if institution not in self.maps:
            with open(os.path.join(self.schema_path, self.path_to_maps[institution]), "r") as file:
                self.maps[institution] = json.load(file)

        return self.maps[institution]

    def load_all_maps(self):
        """
        Will foribly loads all maps defined in institution_mapping.josn
        """

        for key in self.path_to_maps:
            with open(self.path_to_maps[key], "r") as file:
                self.maps[key] = json.load(file)


if __name__ == "__main__":
    mapper()

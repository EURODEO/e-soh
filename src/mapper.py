import json


class mapper():
    """
    Delivers and loads json mappings for netCDF metadata parsing
    Will lazy load maps.
    """

    def __init__(self):
        with open("schemas/institution_mapping.json", "r") as file:
            self.path_to_maps = json.loads(file)

            self.maps = []

    def __call__(self, institution: str) -> dict:
        """
        Call an instance of this class for the correct json mapping given the intitution name.
        Must be same name as the global field from netCDF.
        """
        if institution not in self.maps:
            with open(self.path_to_maps[institution], "r") as file:
                self.maps[institution] = json.loads(file)

        return self.maps[institution]

    def load_all_maps(self):
        """
        Will foribly loads all maps defined in institution_mapping.josn
        """

        for key in self.path_to_maps:
            with open(self.path_to_maps[key], "r") as file:
                self.maps[key] = json.loads(file)

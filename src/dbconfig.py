"""
A configuration file for our database
"""

# a dictionary for our use
version_lookup = {
    "r20r1": {"cdt": "r20r1_dcdm", "vocab": "r20r1_vocabulary", "ref": "r20r1_dref", "cdm": "cdm1.0"},
    "r21r1": {"cdt": "r21r1_dcdm", "vocab": "r21r1_vocabulary", "ref": "r21r1_dref", "cdm": "cdm1.0"},
    "rel_21r2": {"cdt": "rel_21r2_cdm", "vocab": "rel_21r2_vocabulary", "ref": "rel_21r2_ref", "cdm": "cdm1.0"},
    "rel_22r1": {
        "cdt": "rel_22r1_cdm",
        "vocab": "rel_22r1_vocabulary",
        "ref": "rel_22r1_ref",
        "cdm": "cdm1.0",
    },
    "rel_22r2": {"cdt": "rel_22r2_cdm", "vocab": "rel_22r2_vocabulary", "ref": "rel_22r2_ref", "cdm": "cdm2.0"},
    "rel_22r3": {"cdt": "rel_22r3_cdm", "vocab": "rel_22r3_vocabulary", "ref": "rel_22r3_ref", "cdm": "cdm2.0"},
    "rel_22r4": {"cdt": "rel_22r4_cdm", "vocab": "rel_22r4_vocabulary", "ref": "rel_22r4_ref", "cdm": "cdm2.0"},
    "rel_22r5": {"cdt": "rel_22r5_cdm", "vocab": "rel_22r5_vocabulary", "ref": "rel_22r5_ref", "cdm": "cdm2.0"},
    "rel_23r1": {"cdt": "rel_23r1_cdm", "vocab": "rel_23r1_vocabulary", "ref": "rel_23r1_ref", "cdm": "cdm2.0"},
}


# Defining a custom error
class DBVersionError(Exception):
    """Raised when input db version does not exist"""

    def __init__(
        self,
        version,
        message="The data version '{}' is not found in the list of valid versions: \n{}",
    ):
        self.valid_dbversion = version_lookup.keys()
        self.message = message.format(version, list(self.valid_dbversion))
        super().__init__(self.message)
from respark.layer_profile import (
    IntegralColumnProfile,
    profile_integral_column
)


def test_creates_valid_IntegralProfile(employees_df):

    department_id_profile = profile_integral_column(employees_df, "department_id")

    assert isinstance(department_id_profile, IntegralColumnProfile)
    assert department_id_profile.name == "department_id"
    assert department_id_profile.normalised_type == "int"
    assert department_id_profile.nullable is False
    assert department_id_profile.min_value == 1
    assert department_id_profile.max_value == 5
    assert department_id_profile.mean_value == 3


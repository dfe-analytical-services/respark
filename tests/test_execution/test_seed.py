from respark.generate.generator import vary_seed


def test_create_stable_seed_is_deterministic():
    seed = 20100512
    tokens = ("table_name", "column_name", "rule_name")
    different_tokens = ("apple", "orange", "banana")

    first_run = vary_seed(seed, *tokens)
    second_run = vary_seed(seed, *tokens)
    third_run = vary_seed(seed, *different_tokens)

    assert first_run == second_run
    assert first_run != third_run

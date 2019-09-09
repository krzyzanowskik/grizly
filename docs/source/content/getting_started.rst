Getting Started with Grizly
===========================

 .. code:: python

    from grizly import set_cwd, QFrame, join, union
    sq_json = set_cwd("acoe_projects", "blank_project", "blank_subquery.json")
    sq_forecast = QFrame(engine="engine_str"
        ).read_json(json_path=sq_json, subquery="sales_forecast")

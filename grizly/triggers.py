from datetime import datetime, timedelta

class WorkigDayChangeTrigger:

    def __init__():
        pass

    def should_run(self, engine_str, schema, table):

        def get_last_working_day():
            t = datetime.today()
            if t.weekday() == 0: # Monday
                t += timedelta(days=-3)
            else:
                t += timedelta(days=-1)
            return str(t.date())

        last_working_day = get_last_working_day()
        query = f"""
        SELECT Formatdate('yyyy-MM-dd', MAX(transaction_date)) as max
        FROM {schema}.{table}
        WHERE trade_interco_indicator = 'T' AND segment_name = 'Industrial Solutions'
        """
        engine = create_engine(engine_str)
        transaction_date = pd.read_sql(sql=query, con=engine)['max'][0]

        if transaction_date == last_working_day:
            return True
        return False
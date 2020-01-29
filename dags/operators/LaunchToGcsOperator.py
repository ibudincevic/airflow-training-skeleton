from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils import apply_defaults


class LaunchToGcsOperator(SimpleHttpOperator):
    template_fields = ('_ds', '_tomorrow_ds')
    ui_color = '#555’  ui_fgcolor = '  # fff' 

    @apply_defaults
    def __init__(self, query, myvar2, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._query = query
        self._ds = ""
        self._myvar2 = myvar2

    def execute(self, context):

        pass

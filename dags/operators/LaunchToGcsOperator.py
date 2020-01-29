from airflow.operators import BaseOperator
from airflow.utils import apply_defaults


class LaunchToGcsOperator(BaseOperator):
    template_fields = ('_ds', '_tomorrow_ds')
    ui_color = '#555’  ui_fgcolor = '  # fff' 

    @apply_defaults
    def __init__(self, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._query = query
        self._ds = ""
        # self._myvar2 = myvar2

    def execute(self, context):

        pass

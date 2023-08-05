from apache_beam.options.pipeline_options import PipelineOptions
class MyOptions(PipelineOptions):
    """
    カスタムオプション
    引数を渡せるように
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        """Template Methodパターン。このメソッドはPipelineOptionsのインスタンスメソッドで呼ばれているので。

        Args:
            parser (_type_): _description_
        """
        parser.add_argument(
            "--input",
            default="./input.txt",
            help="Input path for the pipeline"
        )

        parser.add_argument(
            "--output",
            default="./output.txt",
            help="Output paht for the pipeline"
        )
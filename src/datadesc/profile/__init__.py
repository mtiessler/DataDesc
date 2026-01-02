from datadesc.profile.overview import OverviewProfiler
from datadesc.profile.schema import SchemaProfiler
from datadesc.profile.missingness import MissingnessProfiler
from datadesc.profile.numeric import NumericProfiler
from datadesc.profile.categorical import CategoricalProfiler
from datadesc.profile.correlations import CorrelationProfiler
from datadesc.profile.uniqueness import UniquenessProfiler

from datadesc.profile.distribution_shape import DistributionShapeProfiler
from datadesc.profile.text_profile import TextProfileProfiler
from datadesc.profile.datetime_profile import DatetimeProfileProfiler
from datadesc.profile.row_missingness import RowMissingnessProfiler
from datadesc.profile.quality_warnings import QualityWarningsProfiler

from datadesc.profile.listlike_profile import ListLikeProfiler
from datadesc.profile.key_duplicates import KeyDuplicatesProfiler

def get_profilers():
    return [
        OverviewProfiler(),
        SchemaProfiler(),
        MissingnessProfiler(),
        NumericProfiler(),
        CategoricalProfiler(),
        CorrelationProfiler(),
        UniquenessProfiler(),
        DistributionShapeProfiler(),
        TextProfileProfiler(),
        DatetimeProfileProfiler(),
        RowMissingnessProfiler(),
        QualityWarningsProfiler(),
        ListLikeProfiler(),
        KeyDuplicatesProfiler(),
    ]

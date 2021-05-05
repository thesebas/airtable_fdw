from collections import namedtuple

import pytest

import multicorn
from airtable_fdw import quals_to_formula

QualMock = namedtuple('Qual', 'field_name,operator,value,is_list_operator,list_any_or_all')
QualMock.__new__.__defaults__ = (None, None, None, False, None)

@pytest.mark.parametrize('quals, expected_formula', [
    ([], ''),
    ([QualMock('pies', '=', 10)], '{pies} = 10'),
    ([QualMock('pies', '<', 10)], '{pies} < 10'),
    ([QualMock('pies', '<=', 10)], '{pies} <= 10'),
    ([QualMock('pies', '>=', 10)], '{pies} >= 10'),
    ([QualMock('pies', '>', 10)], '{pies} > 10'),
    ([QualMock('pies', '=', 'kot')], "{pies} = 'kot'"),
    ([QualMock('pies', '<', 'kot')], "{pies} < 'kot'"),
    ([QualMock('pies', '<=', 'kot')], "{pies} <= 'kot'"),
    ([QualMock('pies', '>=', 'kot')], "{pies} >= 'kot'"),
    ([QualMock('pies', '>', 'kot')], "{pies} > 'kot'"),

    ([QualMock('pies', '>', 'kot'), QualMock('pies', '<', 'kot')], "AND({pies} > 'kot', {pies} < 'kot')"),

    ([QualMock('pies', ('>', True), ['kot', 'krowa'], True, multicorn.ANY)], "OR({pies} > 'kot', {pies} > 'krowa')"),
    ([QualMock('pies', ('>', False), ['koza', 'kaczka'], True, multicorn.ALL)], "AND({pies} > 'koza', {pies} > 'kaczka')"),
    ([QualMock('pies', '=', 10), QualMock('pies', ('>', True), ['kot', 'krowa'], True, multicorn.ANY)], "AND({pies} = 10, OR({pies} > 'kot', {pies} > 'krowa'))"),
    ([QualMock('pies', '=', 10), QualMock('pies', ('>', False), ['koza', 'kaczka'], True, multicorn.ALL)], "AND({pies} = 10, AND({pies} > 'koza', {pies} > 'kaczka'))"),

])
def test_quals_to_formula(quals, expected_formula):
    formula = quals_to_formula(quals)
    assert expected_formula == formula

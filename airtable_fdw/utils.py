import logging

try:
    from multicorn.utils import log_to_postgres as log
except ImportError:
    def log(text, level):
        print("%s: %s" % (level, text))


def first(items, pred):
    return next((i for i in items if pred(i)), None)


def quals_to_formula(quals):
    formula_parts = []
    for q in quals:
        log("quals_to_formula: [%s] [%s] [%s]" % (q.field_name, q.operator, q.value), logging.INFO)
        if q.is_list_operator:
            subformula_parts = []
            operator = q.operator[0]
            for v in q.value:
                value = "'%s'" % v if isinstance(v, str) else v
                subformula_part = '{{{field}}} {operator} {value}'.format(field=q.field_name, operator=operator, value=value)
                subformula_parts.append(subformula_part)

            # formula_part = ('AND(%s)' if q.list_any_or_all == ALL else 'OR(%s)') % ', '.join(subformula_parts) ## not testable
            formula_part = ('AND(%s)' if not q.operator[1] else 'OR(%s)') % ', '.join(subformula_parts)
            formula_parts.append(formula_part)

        elif q.operator in ['=', '<', '>', '>=', '<=', '!=']:
            value = "'%s'" % q.value if isinstance(q.value, str) else q.value
            formula_part = '{{{field}}} {operator} {value}'.format(field=q.field_name, operator=q.operator, value=value)
            formula_parts.append(formula_part)

    if len(formula_parts) == 0:
        return ''
    if len(formula_parts) == 1:
        return formula_parts[0]

    return 'AND(%s)' % (', '.join(formula_parts))

import copy
import re


class Query():
    # LABEL_TO_PROCESSOR_MAP = {
    #     'cat': 'CatDetection',
    #     'dog': 'DogDetection',
    #     'person': 'PersonDetection',
    # }

    obj_attr_re = re.compile('.+?\.(\w+)\=\"(.*?)\"')

    def __init__(self, query=None):
        self.parser = SubscriptionLanguage()
        self._predicates = {}
        self._vekg = None
        self.query_select = None
        self.query_from = None
        self.query_where = None
        self.query_within = None
        self.query = None
        if query:
            self.parse_query(query)

    def _label_to_processor_map(self, label):
        return f'{label.capitalize()}Detection'

    def parse_query(self, query):
        self.query = query
        self.query_select = self.parser.parse_select(query)
        self.query_from = self.parser.parse_from(query)
        self.query_where = self.parser.parse_where(query)
        self.query_within = self.parser.parse_within(query)

    def _get_clause_predicates(self, clause=None):
        if clause is None:
            return []
        if isinstance(clause, str):
            return [clause]
        left_side = clause[0]
        right_side_list = self._get_clause_predicates(clause[-1])
        all_pred = [left_side] + right_side_list
        return all_pred

    def get_predicates_from_clause(self, clause=None, prev=None):
        result = []
        if prev is None:
            prev = {}
            prev.setdefault('object_attribute', {})
        if clause is None:
            return result
        if isinstance(clause, str):
            prev = self._add_clause_to_predicate_dict(clause, prev)
            result = [prev]
            return result

        left_side = clause[0]
        operator = clause[1].upper()
        right_side = clause[2]
        if operator == 'OR':
            prev_copy = copy.deepcopy(prev)
            prev = self._add_clause_to_predicate_dict(left_side, prev)
            result = self.get_predicates_from_clause(clause=right_side, prev=prev_copy)
            result.insert(0, prev)
        elif operator == 'AND':
            prev = self._add_clause_to_predicate_dict(left_side, prev)
            result = self.get_predicates_from_clause(clause=right_side, prev=prev)
        return result

    def get_predicates(self):
        return self.get_predicates_from_clause(self.query_where)

    def _add_clause_to_predicate_dict(self, clause, predicate_dict):
        obj_id, obj_alias = self._get_obj_id_and_alias_matching_predicate(clause)
        attr, value = self.obj_attr_re.match(clause).groups()
        # considering only objects attribute predicates right now
        predicate_dict['object_attribute'].setdefault(obj_id, {})
        predicate_dict['object_attribute'][obj_id][attr] = value
        return predicate_dict

    def _get_obj_id_and_alias_matching_predicate(self, clause_predicate):
        for obj_i, obj_alias in enumerate(self.query_select):
            if obj_alias in clause_predicate:
                return obj_i, obj_alias

    def get_attributes_required_processors(self):
        mat_set = set()
        for predicate in self.get_predicates():
            attr_filter_list = predicate.get('object_attribute', {}).values()
            for attr_filter_dict in attr_filter_list:
                label = attr_filter_dict.get('label', None)
                if label:
                    label
                    processor = self._label_to_processor_map(label)
                # processor = self.LABEL_TO_PROCESSOR_MAP[attr_filter_dict.get('label')]
                    mat_set.add(processor)
        return list(mat_set)

    def get_required_processors(self):
        required_processors = {
            'attribute': self.get_attributes_required_processors(),
            'spatial': [],
            'temporal': []
        }
        return required_processors

    def get_window_info(self):
        if not self.query_within:
            return self.query_within
        return self.query_within[0].lower()

    def __repr__(self):
        return f'Query("{self.query}")'


class SubscriptionLanguage():
    def __init__(self):
        pass

    def clean_query(self, query):
        return query.replace('\n', '')

    def parse_select(self, query):
        query = self.clean_query(query)
        select_re = re.compile('select (.+?) from', re.MULTILINE | re.IGNORECASE)

        select_clauses = select_re.findall(query)
        select_clauses = list(map(str.strip, select_clauses[0].split(',')))
        return select_clauses

    def parse_from(self, query):
        query = self.clean_query(query)
        from_re = re.compile('from (.+?)(?=where|$)', re.MULTILINE | re.IGNORECASE)

        from_clauses = from_re.findall(query.split('within')[0])
        from_clauses = list(map(str.strip, from_clauses[0].split(',')))
        return from_clauses

    def parse_where(self, query):
        query = self.clean_query(query)
        where_re = re.compile('where (.+?)(?=within|$)', re.MULTILINE | re.IGNORECASE)

        where_clauses = where_re.findall(query)
        if not where_clauses:
            return None
        where_clause = where_clauses[0]

        where_clause = where_clause.strip()
        operators = self.parse_operators(where_clause)
        where_clauses = self.parse_operators_to_left_right(operators, where_clause)
        return where_clauses

    def parse_operators_to_left_right(self, operators, clause):
        if not operators:
            return clause
        op = operators.pop()
        left_side, right_side = list(map(str.strip, clause.split(op, 1)))
        right_side = self.parse_operators_to_left_right(operators, right_side)
        return (left_side, op, right_side)

    def parse_operators(self, where_clause):
        where_clause = self.clean_query(where_clause)
        operators_re = re.compile(' (AND|OR) ', re.MULTILINE | re.IGNORECASE)
        available_operators = operators_re.findall(where_clause)
        return available_operators

    def parse_within(self, query):
        query = self.clean_query(query)
        clause_re = re.compile('within (.+)', re.MULTILINE | re.IGNORECASE)

        clauses_match = clause_re.findall(query)
        clauses = list(map(str.strip, clauses_match[0].split(',')))
        return clauses

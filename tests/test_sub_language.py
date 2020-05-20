import unittest

from adaptation_monitor.query_parser import Query, SubscriptionLanguage


class TestSubLanguage(unittest.TestCase):

    def setUp(self):
        self.sub_language = SubscriptionLanguage()

        self.query_text = """
            select object1, object2, object3
            from pub1, pub2
            where
                object1.label="car" AND
                    object2.label="person" OR
                        object3.label="cat" AND object.label="dog"
            within
                TIMEFRAMEWINDOW(10)
        """

    def test_parse_select_single_selection(self):
        query = 'select object from * where object.label="car" within TIMEFRAMEWINDOW(10)'
        selects = self.sub_language.parse_select(query)
        self.assertEqual(selects, ['object'])

    # def test_parse_select_with_cep(self):
    #     query = 'select seq(obj1, obj2) from * where object.label="car" within TIMEFRAMEWINDOW(10)'
    #     selects = self.sub_language.parse_select(query)
    #     self.assertEqual(selects, ['seq(obj1, obj2)'])

    def test_parse_select_multiple_selection(self):
        query = 'select object1, object2 from * where object.label="car" within TIMEFRAMEWINDOW(10)'
        selects = self.sub_language.parse_select(query)
        self.assertEqual(selects, ['object1', 'object2'])

    def test_parse_from_anything_with_where(self):
        query = 'select object, object2 from * where object.label="car" within TIMEFRAMEWINDOW(10)'
        query_from = self.sub_language.parse_from(query)
        self.assertEqual(query_from, ['*'])

    def test_parse_from_anything_without_where(self):
        query = 'select object, object2 from *'
        query_from = self.sub_language.parse_from(query)
        self.assertEqual(query_from, ['*'])

    def test_parse_from_single_publisher_with_where(self):
        query = 'select object, object2 from pub1 where object.label="car" within TIMEFRAMEWINDOW(10)'
        query_from = self.sub_language.parse_from(query)
        self.assertEqual(query_from, ['pub1'])

    def test_parse_from_single_publisher_without_where(self):
        query = 'select object, object2 from pub1 within TIMEFRAMEWINDOW(10)'
        query_from = self.sub_language.parse_from(query)
        self.assertEqual(query_from, ['pub1'])

    def test_parse_from_multiple_publishers_with_where(self):
        query = 'select object, object2 from pub1, pub2 where object.label="car" within TIMEFRAMEWINDOW(10)'
        query_from = self.sub_language.parse_from(query)
        self.assertEqual(query_from, ['pub1', 'pub2'])

    def test_parse_from_multiple_publishers_without_where(self):
        query = 'select object, object2 from pub1, pub2 within TIMEFRAMEWINDOW(10)'
        query_from = self.sub_language.parse_from(query)
        self.assertEqual(query_from, ['pub1', 'pub2'])

    def test_parse_where_empty_where(self):
        query = 'select object, object2 from pub1, pub2 within TIMEFRAMEWINDOW(10)'
        query_where = self.sub_language.parse_where(query)
        self.assertEqual(query_where, None)

    def test_parse_where_single_where(self):
        query = 'select object, object2 from pub1, pub2 where object.label="car" within TIMEFRAMEWINDOW(10)'
        query_where = self.sub_language.parse_where(query)
        self.assertEqual(query_where, 'object.label="car"')

    def test_parse_where_multiple_where(self):
        query = (
            'select object, object2 from pub1, pub2'
            ' where object.label="car" AND object2.label="person" within TIMEFRAMEWINDOW(10)')
        query_where = self.sub_language.parse_where(query)
        self.assertEqual(query_where, ('object.label="car"', 'AND', 'object2.label="person"'))

    def test_parse_operators(self):
        where_clause = 'object.label="car" AND object2.label="person" OR object3.label="cat" AND object.label="dog"'
        operators = self.sub_language.parse_operators(where_clause)
        self.assertEqual(operators, ['AND', 'OR', 'AND'])

    def test_parse_operators_with_and_or_in_the_middle_of_where(self):
        where_clause = 'object.or="car" AND object2.and="person" OR object3.or="cat"'
        operators = self.sub_language.parse_operators(where_clause)
        self.assertEqual(operators, ['AND', 'OR'])

    def test_parse_operators_to_left_right(self):
        operators = ['AND', 'OR', 'AND']
        clause = 'object.label="car" AND object2.label="person" OR object3.label="cat" AND object.label="dog"'
        left_right_operators = self.sub_language.parse_operators_to_left_right(operators, clause)
        expected_result = (
            'object.label="car"',
            'AND',
            (
                'object2.label="person"',
                'OR',
                (
                    'object3.label="cat"',
                    'AND',
                    'object.label="dog"'
                )
            )
        )
        self.assertEqual(left_right_operators, expected_result)

    def test_parse_within(self):
        query_within = self.sub_language.parse_within(self.query_text)
        self.assertEqual(query_within, ['TIMEFRAMEWINDOW(10)'])


class TestQuery(unittest.TestCase):

    def setUp(self):
        self.sub_language = SubscriptionLanguage()

        self.query_text = """
            select object1, object2, object3
            from pub1, pub2
            where
                object1.label="car" AND
                    object2.label="person" OR
                        object3.label="cat" AND object.label="dog"
            within TIMEFRAMEWINDOW(10)
        """

    def test_query_instantiated_correctly_select_clause(self):
        query = Query(self.query_text)

        expected_result = ['object1', 'object2', 'object3']

        self.assertEqual(query.query_select, expected_result)

    def test_query_instantiated_correctly_fromclause(self):
        query = Query(self.query_text)

        expected_result = ['pub1', 'pub2']

        self.assertEqual(query.query_from, expected_result)

    def test_query_instantiated_correctly_where_clause(self):
        query = Query(self.query_text)

        expected_result = (
            'object1.label="car"',
            'AND',
            (
                'object2.label="person"',
                'OR',
                (
                    'object3.label="cat"',
                    'AND',
                    'object.label="dog"'
                )
            )
        )

        self.assertEqual(query.query_where, expected_result)

    def test_get_predicates_from_clause(self):
        query_text = (
            'select object from * '
            'where object.attr1="value1" OR object.attr2="value2" '
            'AND object.attr3="value3" OR object.attr4="value4" '
            'within TIMEFRAMEWINDOW(10)'
        )
        query = Query(query_text)
        predicates = query.get_predicates_from_clause(query.query_where)

        expected_result = [
            {'object_attribute': {0: {'attr1': 'value1'}}},
            {'object_attribute': {0: {'attr2': 'value2', 'attr3': 'value3'}}},
            {'object_attribute': {0: {'attr2': 'value2', 'attr4': 'value4'}}},
        ]
        for i, predicate in enumerate(predicates):
            expected = expected_result[i]
            self.assertDictEqual(predicate, expected)

    def test_get_predicates_correct_for_single_clause(self):
        query_text = 'select object from * where object.something="otherthing" within TIMEFRAMEWINDOW(10)'
        query = Query(query_text)
        predicates = query.get_predicates()

        expected_result = [{'object_attribute': {0: {'something': 'otherthing'}}}]

        for i, predicate in enumerate(predicates):
            expected = expected_result[i]
            self.assertDictEqual(predicate, expected)

    def test_get_predicates_correct_for_two_clause_same_object(self):
        query_text = 'select object from * where object.attr1="value1" AND object.attr2="value2" within TIMEFRAMEWINDOW(10)'
        query = Query(query_text)
        predicates = query.get_predicates()

        expected_result = [{
            'object_attribute': {
                0: {
                    'attr1': 'value1',
                    'attr2': 'value2',
                }
            }
        }]
        for i, predicate in enumerate(predicates):
            expected = expected_result[i]
            self.assertDictEqual(predicate, expected)

    def test_get_predicates_correct_for_two_clause_diff_objects(self):
        query_text = 'select object1, object2 from * where object1.attr1="value1" AND object2.attr2="value2" within TIMEFRAMEWINDOW(10)'
        query = Query(query_text)
        predicates = query.get_predicates()

        expected_result = [{
            'object_attribute': {
                0: {
                    'attr1': 'value1',
                },
                1: {
                    'attr2': 'value2',
                }
            }
        }]
        for i, predicate in enumerate(predicates):
            expected = expected_result[i]
            self.assertDictEqual(predicate, expected)

    def test_get_predicates_correct_for_two_mixed_clause_for_each_diff_objects(self):
        query_text = """
            select object1, object2
            from *
            where
                object1.attr1="value1" AND
                    object2.attr3="value3" AND

                        object1.attr2="value2" AND
                            object2.attr4="value4" AND
            within TIMEFRAMEWINDOW(10)
        """
        query = Query(query_text)
        predicates = query.get_predicates()

        expected_result = [{
            'object_attribute': {
                0: {
                    'attr1': 'value1',
                    'attr2': 'value2',
                },
                1: {
                    'attr3': 'value3',
                    'attr4': 'value4',
                }
            }
        }]
        for i, predicate in enumerate(predicates):
            expected = expected_result[i]
            self.assertDictEqual(predicate, expected)

    def test_get_predicates_correct_for_query_all(self):
        query = Query("select * from * within TIMEFRAMEWINDOW(10)")

        predicates = query.get_predicates()

        expected_result = [{'object_attribute': {}}]
        for i, predicate in enumerate(predicates):
            expected = expected_result[i]
            self.assertDictEqual(predicate, expected)

    def test_get_required_processors_with_single_processor(self):
        query_text = 'select object1 from * where object1.label="cat" within TIMEFRAMEWINDOW(10)'
        query = Query(query_text)
        processors = query.get_required_processors()

        expected_result = {
            'attribute': ['CatDetection'],
            'spatial': [],
            'temporal': []
        }
        for proc_type, processors in query.get_required_processors().items():
            expected = sorted(expected_result[proc_type])
            self.assertListEqual(sorted(processors), expected)

    def test_get_required_processors_with_multiple_processor(self):
        query_text = 'select object1, object2 from * where object1.label="cat" and object2.label="person" within TIMEFRAMEWINDOW(10)'
        query = Query(query_text)
        # processors = sorted()
        expected_result = {
            'attribute': ['PersonDetection', 'CatDetection'],
            'spatial': [],
            'temporal': []
        }
        for proc_type, processors in query.get_required_processors().items():
            expected = sorted(expected_result[proc_type])
            self.assertListEqual(sorted(processors), expected)
        # self.assertDictEqual(processors, expected_result)

    # def test_strange_query(self):
    #     query_text = "select object_detection from publisher 1 where (object.label = 'car') within TIMEFRAMEWINDOW(10) withconfidence >50"

    #     query = Query(query_text)
    #     import ipdb; ipdb.set_trace()
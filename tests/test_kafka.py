from tests.conftest import Check


def test_consume_records(mock_consumer, kafka_client, check_data):
    """Тестирование получения данных."""
    batch_size = 5
    record_batches_gen = kafka_client.consume_records(batch_size_before_insert=batch_size)
    record_batches = list(record_batches_gen)
    assert len(record_batches) > 0
    for batch in record_batches:
        assert batch == [Check(**check_data)]


def test_consume_record(mock_consumer, kafka_client, check_data):
    """Тестирование получения одной записи."""
    check = list(kafka_client._consume_record())
    assert len(list(check)) == 1
    assert list(check[0])[0] == Check(**check_data)

import os
import pytest


@pytest.mark.vcr
def test_fetch_unsubscribed(client):
    unsubscribed_data = client.fetch_unsubscribed()
    for unsubscribed in unsubscribed_data["data"]:
        assert "id" in unsubscribed
        assert "email" in unsubscribed
        assert "unsubscribetime" in unsubscribed
        assert "emailstate" in unsubscribed
        assert "emailreason" in unsubscribed


@pytest.mark.vcr
def test_fetch_responses(client):
    page = 1
    page_size = 50
    # Uses a UNIX timestamp
    start_time_utc = "2018-01-01T00:00:00Z"
    end_time_utc = "2020-01-01T00:00:00Z"
    res = client.fetch_responses(page, page_size, start_time_utc, end_time_utc)

    responses = res["data"]

    for response in responses:
        assert "response_id" in response
        assert "person_id" in response
        assert "contact_id" in response
        assert "name" in response
        assert "email" in response
        assert "answer" in response
        assert "answerlabel" in response
        assert "data" in response
        assert "comment" in response
        assert "note" in response
        assert "status" in response
        assert "dontcontact" in response
        assert "sent" in response
        assert "opened" in response
        assert "responded" in response
        assert "lastemailed" in response
        assert "created" in response
        assert "segment" in response
        assert "question_type" in response
        assert "published" in response
        assert "publishedname" in response
        assert "publishedavatar" in response
        assert "deliverymethod" in response
        assert "survey_template" in response
        assert "theme" in response
        assert "life_cycle" in response
        assert "customproperty_c" in response
        assert "topic_c" in response
        assert "workflow_zendesk_detractor_c" in response
        assert "workflow_custom_alerts_mary_c" in response
        assert "workflow_email_alert_c" in response
        assert "dashboard" in response
        assert "email_token" in response


@pytest.mark.vcr
def test_fetch_contact(client):
    # get a contact_id
    page = 1
    page_size = 50
    # Uses a UNIX timestamp
    start_time_utc = "2018-01-01T00:00:00Z"
    end_time_utc = "2020-01-01T00:00:00Z"
    responses = client.fetch_responses(page, page_size, start_time_utc, end_time_utc)

    contact_id = responses.get("data")[0]["contact_id"]

    response = client.fetch_contact(contact_id)

    contact = response["data"]

    assert "id" in contact
    assert "lastemailed" in contact
    assert "active" in contact
    assert "created" in contact
    assert "scheduled" in contact
    assert "firstname" in contact
    assert "lastname" in contact
    assert "name" in contact
    assert "email" in contact
    assert "company_id" in contact
    assert "segment" in contact
    assert "unsubscribetime" in contact
    assert "source" in contact
    assert "sourceid" in contact
    assert "importedattime" in contact
    assert "intercomid" in contact
    assert "imported" in contact
    assert "profiled" in contact
    assert "profile" in contact
    assert "avatar" in contact
    assert "publishedavatar" in contact
    assert "publishedavatarcolor" in contact
    assert "deleted" in contact
    assert "emailstate" in contact
    assert "intercomsegmentid" in contact
    assert "surveycount" in contact
    assert "delayminutes" in contact
    assert "emailreason" in contact
    assert "scheduler_id" in contact
    assert "elastic_at" in contact
    assert "life_cycle" in contact
    assert "sfaccountid" in contact
    assert "dynamicscontactid" in contact
    assert "responsetime" in contact
    assert "channel" in contact
    assert "sflastsenthash" in contact
    assert "sflastsenttime" in contact
    assert "sfcontactid" in contact
    assert "integrationsegment_id" in contact
    assert "importer_id" in contact
    assert "sfcaseid" in contact
    assert "sfleadid" in contact
    assert "applicationid" in contact
    assert "mc_contact_identifier" in contact
    assert "event" in contact
    assert "triggeremail" in contact
    assert "obeyrules" in contact
    assert "thendeactivate" in contact
    assert "customproperty_c" in contact


@pytest.mark.vcr()
def test_fetch_sent_statistics(client):
    rolling_day = 30  # 30 is default

    sent_stats = client.fetch_sent_statistics(rolling_history=rolling_day)
    assert "nps" in sent_stats
    assert "sent" in sent_stats
    assert "delivered" in sent_stats
    assert "opened" in sent_stats
    assert "responded" in sent_stats
    assert "promoters" in sent_stats
    assert "passives" in sent_stats
    assert "detractors" in sent_stats
    assert "responserate" in sent_stats


@pytest.mark.vcr()
def test_fetch_historical_stats(client):
    date = "2020-01-01"

    historical_stats = client.fetch_historical_stats(date=date)["data"][0]
    assert "year" in historical_stats
    assert "month" in historical_stats
    assert "day" in historical_stats
    assert "weekday" in historical_stats
    assert "sent" in historical_stats
    assert "delivered" in historical_stats
    assert "opened" in historical_stats
    assert "responded" in historical_stats
    assert "promoters" in historical_stats
    assert "passives" in historical_stats
    assert "detractors" in historical_stats
    assert "nps" in historical_stats
    assert "comments" in historical_stats
    assert "comment_length" in historical_stats
    assert "comment_responded_percent" in historical_stats
    assert "surveys_responded_percent" in historical_stats
    assert "surveys_delivered_responded_percent" in historical_stats
    assert "surveys_opened_responded_percent" in historical_stats

@pytest.mark.vcr()
def test_fetch_nps(client):
    days = 30

    nps_score = client.fetch_nps(days)

    assert "NPS" in nps_score

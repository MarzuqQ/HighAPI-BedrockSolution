resource "aws_cloudwatch_metric_alarm" "bedrock_errors" {
  alarm_name          = "bedrock-invocation-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = var.alarm_evaluation_periods
  metric_name         = "Invocation4XXErrors"
  namespace           = "AWS/Bedrock"
  period              = 300
  statistic           = "Sum"
  threshold           = var.alarm_threshold
  alarm_description   = "Alert when Bedrock API invocation errors exceed threshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarm.arn]
}

resource "aws_sns_topic" "alarm" {
  name         = "bedrock-errors-alarm"
  display_name = "Bedrock Errors Alarm"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alarm.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

resource "aws_cloudwatch_dashboard" "bedrock" {
  dashboard_name = var.dashboard_name
  dashboard_body = jsonencode({
    widgets = [
      {
        height = 6
        width  = 12
        type   = "metric"
        properties = {
          metrics = [
            [
              "AWS/Bedrock",
              "Invocation4XXErrors",
              {
                stat = "Sum"
              }
            ]
          ]
          period  = 300
          title   = "Bedrock API Errors"
          region  = data.aws_region.current.name
          view    = "timeSeries"
          stacked = false
        }
      }
    ]
  })
}

data "aws_region" "current" {}
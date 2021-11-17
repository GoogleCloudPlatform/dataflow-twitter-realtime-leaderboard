"""
Copyright 2021 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import re
import argparse
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery


def frequency_schema(field_name):
    return {
        'fields': [{
            'name': field_name, 'type': 'STRING', 'mode': 'NULLABLE'
        }, {
            'name': 'count', 'type': 'NUMERIC', 'mode': 'NULLABLE'
        }]
    }

class RemoveUrls(beam.DoFn):
    def process(self, tweet):
        pattern = 'http\S+'
        tweet['song'] = re.sub(pattern, '', tweet['song'], flags=re.IGNORECASE)
        tweet['artist'] = re.sub(pattern, '', tweet['artist'], flags=re.IGNORECASE)

        yield tweet

class RemoveHashtags(beam.DoFn):
    def process(self, tweet):
        pattern = '#\S+'
        tweet['song'] = re.sub(pattern, '', tweet['song'])

        yield tweet

class RemoveSource(beam.DoFn):
    def process(self, tweet):
        pattern = '(on|at) (#|@)\S+'
        tweet['song'] = re.sub(pattern, '', tweet['song'], flags=re.IGNORECASE)
        tweet['artist'] = re.sub(pattern, '', tweet['artist'], flags=re.IGNORECASE)

        yield tweet

class RemoveExtraChars(beam.DoFn):
    def process(self, tweet):
        extra_chars = ['"', ':', ';', '|', '“', '”', '`', '~', '(', ')', '[', ']', '♫',]

        for char in extra_chars:
            tweet['artist'] = tweet['artist'].replace(char, '')
            tweet['song'] = tweet['song'].replace(char, '')

        tweet['artist'] = tweet['artist'].strip()
        tweet['song'] = tweet['song'].strip()

        yield tweet

class RemoveStopPatterns(beam.DoFn):
    def process(self, tweet):
        stop_patterns = [
                'Listen to this track and more',
                'via', 
                'official video', 
                'tune in', 
                'listen live', 
                'listen now',
                '♫ at',
                'Listen:',
                '#listen',
                '\(Official Music Video\)',
                '\(Official Lyric Video\)',
                '\(Official Audio\)',
                ]

        for pattern in stop_patterns: 
            tweet['song'] = re.sub(pattern, '', tweet['song'], flags=re.IGNORECASE)
            tweet['artist'] = re.sub(pattern, '', tweet['artist'], flags=re.IGNORECASE)

        yield tweet

class ParseTweet(beam.DoFn):
    def process(self, text):
        tweet = {}
        split_ts_from_tweet = re.split('#nowplaying', text, flags=re.IGNORECASE)
        tweet['date_time'] = split_ts_from_tweet[0].strip()
        tweet['raw_tweet'] = split_ts_from_tweet[1].strip() 

        tweet['song'] = ''
        tweet['artist'] = ''

        if re.search('by', tweet['raw_tweet'], flags=re.IGNORECASE):
            split_tweet = re.split('by', tweet['raw_tweet'], flags=re.IGNORECASE)
            tweet['song'] = split_tweet[0].strip()
            tweet['artist'] = split_tweet[1].strip()

        elif '-' in tweet['raw_tweet']:
            split_tweet = tweet['raw_tweet'].split('-')
            tweet['song'] = split_tweet[1].strip()
            tweet['artist'] = split_tweet[0].strip()

        yield tweet

class ValidateTweet(beam.DoFn):
    OUTPUT_TAG_REJECTS = 'tag_tweet_rejects'
    PATTERN = re.compile('^(\w{3,}\s+\d{2}\s+\d{2}:\d{2}:\d{2})\s+#nowplaying\s+(?P<s1>.*)\s+(by|-)\s+(?P<s2>.*)', re.IGNORECASE)

    def process(self, tweet):
        if (self.PATTERN.match(tweet)):
            yield tweet
        else:
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_REJECTS, tweet)

def run(argv=None, save_main_session=True):
    """Main entry point for the pipeline"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        help='Input file to process')
    parser.add_argument(
        '--output',
        dest='output',
        help='Destination for the output file')
    parser.add_argument(
        '--bqdataset',
        dest='bqdataset',
        help='BigQuery target dataset')
    parser.add_argument(
        '--bqproject',
        dest='bqproject',
        help='GCP bq project')

    args, beam_args = parser.parse_known_args()

    beam_options = PipelineOptions(beam_args)
    beam_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=beam_options) as p:
        lines = p | ReadFromText(args.input)

        filtered_tweets = (
                lines 
                | beam.ParDo(ValidateTweet()).with_outputs(
                    ValidateTweet.OUTPUT_TAG_REJECTS, main='tweets'
                ))

        (filtered_tweets[ValidateTweet.OUTPUT_TAG_REJECTS] | 'Write rejects' >> WriteToText(args.output))

        songs_and_artists = (
                filtered_tweets['tweets']
                | 'Parse Tweet' >> beam.ParDo(ParseTweet())
                | 'Remove URLs' >> beam.ParDo(RemoveUrls())
                | 'Remove Source' >> beam.ParDo(RemoveSource())
                | 'Remove Song Hashtags' >> beam.ParDo(RemoveHashtags())
                | 'Remove stop patterns' >> beam.ParDo(RemoveStopPatterns())
                | 'Remove extra chars' >> beam.ParDo(RemoveExtraChars())
            )

        song_frequency = (
            songs_and_artists 
            | 'Extract song field' >> beam.Map(lambda tweet: (tweet['song'], 1))
            | 'Group and sum songs' >> beam.CombinePerKey(sum)
            | 'Map songs to dict' >> beam.Map(lambda element: {'song': element[0], 'count': element [1]})
        )

        artist_frequency = (
            songs_and_artists 
            | 'Extract artist field' >> beam.Map(lambda tweet: (tweet['artist'], 1))
            | 'Group and sum artists' >> beam.CombinePerKey(sum)
            | 'Map artists to dict' >> beam.Map(lambda element: {'artist': element[0], 'count': element [1]})
        )

        freq_schema_song = frequency_schema('song')

        (song_frequency 
            | 'Write song frequency to BQ' >> beam.io.WriteToBigQuery(
                bigquery.TableReference(projectId=args.bqproject, datasetId=args.bqdataset, tableId='songs'),
                schema=freq_schema_song,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

        freq_schema_artist = frequency_schema('artist')

        (artist_frequency 
            | 'Write artist frequency to BQ' >> beam.io.WriteToBigQuery(
                bigquery.TableReference(projectId=args.bqproject, datasetId=args.bqdataset, tableId='artists'),
                schema=freq_schema_artist,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

if __name__ == '__main__':
    run()

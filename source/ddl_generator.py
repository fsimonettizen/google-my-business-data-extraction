import sys
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ddl_validate(conn):
    '''
        Validate DDL for GMB data
    '''

    with conn.cursor() as cur:

        print("ddl_generator | public.gmb_location")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public."gmb_location" (
                "name" varchar NOT NULL,
                language_code varchar NOT NULL,
                store_code varchar NULL,
                category_id varchar NULL,
                display_name varchar NULL,
                location_name varchar NULL,
                primary_phone varchar NULL,
                longitude float8 NULL,
                latitude float8 NULL,
                address_lines varchar NULL,
                administrative_area varchar NULL,
                locality varchar NULL,
                postal_code varchar NULL,
                region_code varchar NULL,
                sub_locality varchar NULL,
                PRIMARY KEY ("name")
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS location_name_idx ON public.gmb_location USING btree ("name");
        """)

        print("ddl_generator | public.gmb_location_metrics")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.gmb_location_metrics (
                "name" varchar NOT NULL,
                start_time timestamp NULL,
                queries_direct int4 NULL,
                queries_indirect int4 NULL,
                queries_chain int4 NULL,
                views_maps int4 NULL,
                views_search int4 NULL,
                actions_website int4 NULL,
                actions_phone int4 NULL,
                actions_driving_directions int4 NULL,
                photos_count_merchant int4 NULL,
                photos_views_merchant int4 NULL,
                photos_views_customers int4 NULL,
                photos_count_customers int4 NULL,
                local_post_views_search int4 NULL,
                local_post_actions_call_to_action int4 NULL,
                PRIMARY KEY ("name", start_time)
            );
        """)

        print("ddl_generator | index public.gmb_location_metrics_name_idx")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS location_metrics_name_idx ON public.gmb_location_metrics USING btree (name, start_time);
        """)


        print("ddl_generator | public.gmb_driving_request_metrics")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS public.gmb_driving_request_metrics (
                "name" varchar NULL,
                "start_time" timestamp NULL,
                latitude float8 NULL,
                longitude float8 NULL,
                "label" varchar NULL,
                count int4 NULL,
                PRIMARY KEY ("name", start_time, "label")
            );
        ''')
        print("ddl_generator | gmb_driving_request_metrics_name_idx")
        cur.execute('''
            CREATE INDEX IF NOT EXISTS gmb_driving_request_metrics_name_idx ON public.gmb_driving_request_metrics USING btree (name, start_time);
        ''')

        print("ddl_generator | public.gmb_reviews")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS public.gmb_reviews (
                id INT GENERATED ALWAYS AS IDENTITY,
                "name" varchar NOT NULL,
                "review_name" varchar NOT NULL,
                display_name varchar NULL,
                profile_photo_url varchar NULL,
                star_rating varchar NULL,
                star_rating_int int4 NULL,
                "comment" varchar NULL,
                "text_pt_br" varchar NULL,
                "text_en" varchar NULL,
                "create_time" timestamp NULL,
                "update_time" timestamp NULL,
                has_words_count_processed boolean DEFAULT False,
                has_aws_compreend_sentiment_processed boolean DEFAULT False,
                has_aws_compreend_name_entity_processed boolean DEFAULT False,
                has_aws_compreend_key_phrase_processed boolean DEFAULT False,
                PRIMARY KEY ("name", "review_name", create_time)  
            );
        ''')

        print("ddl_generator | gmb_reviews_name_idx")
        cur.execute('''
            CREATE INDEX IF NOT EXISTS gmb_reviews_name_idx ON public.gmb_reviews USING btree (name, "review_name", "review_name"); 
        ''')

        print("ddl_generator | public.gmb_questions")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS public.gmb_questions (
                id INT GENERATED ALWAYS AS IDENTITY,
                "name" varchar NULL,
                "question_name" varchar NULL,
                display_name varchar NULL,
                profile_photo_url varchar NULL,
                "type" varchar NULL,
                "text_pt_br" varchar NULL,
                "text_en" varchar NULL,
                "text" varchar NULL,
                create_time timestamp NULL,
                "update_time" timestamp NULL,
                has_words_count_processed boolean DEFAULT False,
                has_aws_compreend_sentiment_processed boolean DEFAULT False,
                has_aws_compreend_name_entity_processed boolean DEFAULT False,
                has_aws_compreend_key_phrase_processed boolean DEFAULT False,
                PRIMARY KEY ("name", "question_name") 
            );
        ''')

        print("ddl_generator | gmb_questions_name_idx")
        cur.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS gmb_questions_name_idx ON public.gmb_questions USING btree (name, question_name);
        ''')

        print("ddl_generator | public.gmb_answers")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS public.gmb_answers (
                id INT GENERATED ALWAYS AS IDENTITY,
                "name" varchar NULL,
                "question_name" varchar NULL,
                "answers_name" varchar NULL,
                display_name varchar NULL,
                profile_photo_url varchar NULL,
                "type" varchar NULL,
                "text_pt_br" varchar NULL,
                "text_en" varchar NULL,
                "text" varchar NULL,
                create_time timestamp NULL,
                "update_time" timestamp NULL,
                top_answer boolean NULL,
                has_words_count_processed boolean DEFAULT False,
                has_aws_compreend_sentiment_processed boolean DEFAULT False,
                has_aws_compreend_name_entity_processed boolean DEFAULT False,
                has_aws_compreend_key_phrase_processed boolean DEFAULT False,
                PRIMARY KEY ("name", "question_name", "answers_name") 
            );
        ''')

        print("ddl_generator | gmb_answers_name_idx")
        cur.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS gmb_answers_name_idx ON public.gmb_answers USING btree (name, question_name, answers_name);
        ''')

        # cur.execute('''
        #     CREATE OR REPLACE VIEW location_metrics_basic AS
        #         SELECT l.location_name, lm.queries_direct, lm.queries_indirect, lm.queries_chain, lm.views_maps, lm.views_search, lm.actions_website, lm.actions_phone, lm.actions_driving_directions, lm.photos_count_merchant, lm.photos_views_merchant, lm.photos_views_customers, lm.photos_count_customers, lm.local_post_views_search, lm.local_post_actions_call_to_action
        #         FROM "location" l, location_metrics lm 
        #         WHERE l.name = lm.name ;
        # ''')
        print("ddl_generator | public.custom_words_count")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS public.custom_words_count (
                "name" varchar NULL,
                origin_entity varchar NULL,
                origin_name varchar NULL,
                word varchar NULL,
                start_time timestamp NULL,
                user_type varchar NULL,
                "external_row_id" timestamp NULL,
                PRIMARY KEY ("name", "start_time", origin_entity, word) 
            );
        ''')

        print("ddl_generator | answers_name_idx")
        cur.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS answers_name_idx ON public.custom_words_count USING btree ("name", start_time, origin_entity);
        ''')

        # Execute aws compreend structure 
        # Avaible: sentiment, name entity, key_phrase 
        # 
        print("ddl_generator | public.aws_compreend_sentiment")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS public.aws_compreend_sentiment (
                origin_entity varchar NULL,
                origin_id int4,
                sentiment varchar NULL,
                mixed float8,
                positive float8,
                neutral float8,
                negative float8,
                PRIMARY KEY ("origin_entity", "origin_id", "sentiment") 
            );
        ''')

        print("ddl_generator | aws_compreend_sentiment_idx")
        cur.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS aws_compreend_sentiment_idx ON public.aws_compreend_sentiment USING btree (origin_entity, origin_id, sentiment);
        ''')

        print("ddl_generator | public.aws_compreend_entities")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS public.aws_compreend_entities (
                origin_entity varchar NULL,
                origin_id int4,
                "text" varchar,
                score float8,
                type varchar,
                begin_offset int4,
                end_offset int4,
                PRIMARY KEY ("origin_entity", "origin_id", "text")
            );
        ''')

        print("ddl_generator | aws_compreend_entities_idx")
        cur.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS aws_compreend_entities_idx ON public.aws_compreend_entities USING btree (origin_entity, origin_id, "text");
        ''')

        print("ddl_generator | public.aws_compreend_key_phrase")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS public.aws_compreend_key_phrase (
                origin_entity varchar NULL,
                origin_id int4,
                "text" varchar,
                score float8,
                type varchar,
                begin_offset int4,
                end_offset int4,
                PRIMARY KEY ("origin_entity", "origin_id", "text")
            );
        ''')

        print("ddl_generator | aws_compreend_key_phrase_idx")
        cur.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS aws_compreend_key_phrase_idx ON public.aws_compreend_key_phrase USING btree (origin_entity, origin_id, "text");
        ''')

        print("ddl_generator | public.base_parameters")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS public.base_parameters (
                "name" varchar NULL,
                "value" varchar NULL,
                PRIMARY KEY ("name")
            );
        ''')

        print("ddl_generator | base_parameters_idx")
        cur.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS base_parameters_idx ON public.base_parameters USING btree ("name");
        ''')

        # print("ddl_generator | base_parameters | creation")
        # try:
        #     cur.execute('''
        #         INSERT INTO public.base_parameters ("name", "value") VALUES ('gmb_reviews_extraction_last_date', '');
        #         INSERT INTO public.base_parameters ("name", "value") VALUES ('gmb_questions_extraction_last_date', '');
        #         INSERT INTO public.base_parameters ("name", "value") VALUES ('gmb_answers_extraction_last_date', '');
        #         INSERT INTO public.base_parameters ("name", "value") VALUES ('gmb_insights_extraction_last_date', '');
        #         INSERT INTO public.base_parameters ("name", "value") VALUES ('gmb_driving_request_extraction_last_date', '');
        #     ''')
        # except Exception as e:
        #     print("ddl_generator | base_parameters | already created")
        #     pass

        #
        #
        # Exemplod e vires (Donut Chart (discovery, indirect and chain))
        #
        # 
        cur.execute('''
            CREATE OR REPLACE VIEW discovery AS
                SELECT l.location_name, 'Pesquisa por marca' AS "discovery_mode", SUM(lm.queries_chain) AS "total", '1' AS "month"
                FROM gmb_location_metrics lm, "gmb_location" l
                WHERE lm.start_time >= now() - interval '1 month'
                    AND lm.name = l.name 
                GROUP BY l.location_name
                UNION ALL
                SELECT l.location_name, 'Pesquisa direta' AS "discovery_mode", SUM(lm.queries_direct) AS "total", '1' AS "month"
                FROM gmb_location_metrics lm, "gmb_location" l
                WHERE lm.start_time >= now() - interval '1 month'
                    AND lm.name = l.name
                    GROUP BY l.location_name
                UNION ALL
                SELECT l.location_name, 'Pesquisa Indireta' AS "discovery_mode", SUM(lm.queries_indirect) AS "total", '1' AS "month"
                FROM gmb_location_metrics lm, "gmb_location" l
                WHERE lm.start_time >= now() - interval '1 month'
                    AND lm.name = l.name
                    GROUP BY l.location_name
                UNION ALL
                SELECT 'Todos' AS "location_name", 'Pesquisa Indireta' AS "discovery_mode", SUM(lm.queries_indirect) AS "total", '1' AS "month"
                FROM gmb_location_metrics lm
                WHERE lm.start_time >= now() - interval '1 month'
                    GROUP BY location_name
                UNION ALL
                SELECT 'Todos' AS "location_name", 'Pesquisa direta' AS "discovery_mode", SUM(lm.queries_direct) AS "total", '1' AS "month"
                FROM gmb_location_metrics lm
                WHERE lm.start_time >= now() - interval '1 month'
                    GROUP BY location_name
                UNION ALL
                SELECT 'Todos' AS "location_name", 'Pesquisa por marca' AS "discovery_mode", SUM(lm.queries_chain) AS "total", '1' AS "month"
                FROM gmb_location_metrics lm
                WHERE lm.start_time >= now() - interval '1 month'
                    GROUP BY location_name
                UNION ALL
                SELECT l.location_name, 'Pesquisa por marca' AS "discovery_mode", SUM(lm.queries_chain) AS "total", '1' AS "month"
                FROM gmb_location_metrics lm, "gmb_location" l
                WHERE lm.start_time >= now() - interval '2 month'
                    AND lm.name = l.name 
                GROUP BY l.location_name
                UNION ALL
                SELECT l.location_name, 'Pesquisa direta' AS "discovery_mode", SUM(lm.queries_direct) AS "total", '2' AS "month"
                FROM gmb_location_metrics lm, "gmb_location" l
                WHERE lm.start_time >= now() - interval '2 month'
                    AND lm.name = l.name
                    GROUP BY l.location_name
                UNION ALL
                SELECT l.location_name, 'Pesquisa Indireta' AS "discovery_mode", SUM(lm.queries_indirect) AS "total", '2' AS "month"
                FROM gmb_location_metrics lm, "gmb_location" l
                WHERE lm.start_time >= now() - interval '2 month'
                    AND lm.name = l.name
                    GROUP BY l.location_name
                UNION ALL
                SELECT 'Todos' AS "location_name", 'Pesquisa Indireta' AS "discovery_mode", SUM(lm.queries_indirect) AS "total", '2' AS "month"
                FROM gmb_location_metrics lm
                WHERE lm.start_time >= now() - interval '2 month'
                    GROUP BY location_name
                UNION ALL
                SELECT 'Todos' AS "location_name", 'Pesquisa direta' AS "discovery_mode", SUM(lm.queries_direct) AS "total", '2' AS "month"
                FROM gmb_location_metrics lm
                WHERE lm.start_time >= now() - interval '2 month'
                    GROUP BY location_name
                UNION ALL
                SELECT 'Todos' AS "location_name", 'Pesquisa por marca' AS "discovery_mode", SUM(lm.queries_chain) AS "total", '2' AS "month"
                FROM gmb_location_metrics lm
                WHERE lm.start_time >= now() - interval '2 month'
                    GROUP BY location_name
                UNION ALL
                SELECT l.location_name, 'Pesquisa por marca' AS "discovery_mode", SUM(lm.queries_chain) AS "total", '1' AS "month"
                FROM gmb_location_metrics lm, "gmb_location" l
                WHERE lm.start_time >= now() - interval '3 month'
                    AND lm.name = l.name 
                GROUP BY l.location_name
                UNION ALL
                SELECT l.location_name, 'Pesquisa direta' AS "discovery_mode", SUM(lm.queries_direct) AS "total", '3' AS "month"
                FROM gmb_location_metrics lm, "gmb_location" l
                WHERE lm.start_time >= now() - interval '3 month'
                    AND lm.name = l.name
                    GROUP BY l.location_name
                UNION ALL
                SELECT l.location_name, 'Pesquisa Indireta' AS "discovery_mode", SUM(lm.queries_indirect) AS "total", '3' AS "month"
                FROM gmb_location_metrics lm, "gmb_location" l
                WHERE lm.start_time >= now() - interval '3 month'
                    AND lm.name = l.name
                    GROUP BY l.location_name
                UNION ALL
                SELECT 'Todos' AS "location_name", 'Pesquisa Indireta' AS "discovery_mode", SUM(lm.queries_indirect) AS "total", '3' AS "month"
                FROM gmb_location_metrics lm
                WHERE lm.start_time >= now() - interval '3 month'
                    GROUP BY location_name
                UNION ALL
                SELECT 'Todos' AS "location_name", 'Pesquisa direta' AS "discovery_mode", SUM(lm.queries_direct) AS "total", '3' AS "month"
                FROM gmb_location_metrics lm
                WHERE lm.start_time >= now() - interval '3 month'
                    GROUP BY location_name
                UNION ALL
                SELECT 'Todos' AS "location_name", 'Pesquisa por marca' AS "discovery_mode", SUM(lm.queries_chain) AS "total", '3' AS "month"
                FROM gmb_location_metrics lm
                WHERE lm.start_time >= now() - interval '3 month'
                    GROUP BY location_name
        ''')
        cur.execute('COMMIT;')
        return True
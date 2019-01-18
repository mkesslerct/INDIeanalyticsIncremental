split_duration <- function(date, duration){
  ## given a date (start of the session) and the duration of the session, the
  ## output is a tibble, with all the days the session spans over, and the
  ## corresponding duration of the session these days.
  ##
  days <- seq(
    from = lubridate::floor_date(date, unit = "day"),
    to = lubridate::floor_date(date + duration, unit = "day"),
    by = "day"
  )
  dates_auxiliary <- c(
    date,
    seq(
      from = lubridate::ceiling_date(date, unit = "day"),
      by = "day",
      length.out = (length(days) - 1)
    ),
    date + duration
  )
  tibble::tibble(day = days, duration = diff(dates_auxiliary))
}
##
## ----------------------------------------------------------------------------
##
## Prepares a list to send to the highcharts API, through hc_add_series_list
##
## -----------------------------------------------------------------------------
##
##
colors_q <- setNames(
  c("black", "blue", "green"),
  c("q1", "q2", "q3")
)
prepare_path <- function(df){
  list(
    data = purrr::pmap(
      list(df$quartile, df$percentage, df$n_q, df$variable),
      function(a, b, c, d) list(
        x = a,
        y = b,
        n = c,
        name = d,
        marker = list(enabled = FALSE)
      )
    ),
    name = df$variable[1L],
    color = setNames(colors_q[df$variable[1L]], NULL),
    step = "left"
  )
}

prepare_intermediate <- function(intermediate){
    ## sets the type of columns associated to objects in intermediate
    if (length(intermediate$last_event) > 0){
        intermediate$last_event <-
            intermediate$last_event %>%
            dplyr::mutate(
                       date_last_event = lubridate::ymd_hms(date_last_event),
                       time_spent = as.double(time_spent),
                       percentage = as.double(percentage)
                   )
    }
    if (!is.null(intermediate$user_url_time)){
          intermediate$user_url_time <-
            intermediate$user_url_time %>%
            dplyr::mutate_at(
                       dplyr::vars(percentage:time_spent),
                       .funs = dplyr::funs(as.double)
                   )
    }
    if (!is.null(intermediate$daily_effort)){
        intermediate$daily_effort <-
            intermediate$daily_effort %>%
            dplyr::mutate(
                       day = lubridate::ymd(day, tz = "UTC"),
                       time_spent = as.double(time_spent)
                   )
    }
    intermediate
}

prepare_events <- function(events, intermediate = NULL){
  ## manipulates events as read from json, adding duration etc...
  ## date is converted to datetime object
  ## percentage is converted to numeric
  ## The column time_spent is added, which measures, for each event,
  ## the time spent by the user in the url, up to the current event
  ## The column duration_session is added, which indicates the duration of the
  ## session the event belongs to
    ##
    events <- events %>%
    dplyr::mutate(
      percentage = as.numeric(percentage),
      date = lubridate::mdy_hms(date)
    ) %>%
    dplyr::arrange(date) %>% #arrange(date, type_id) %>% (type id para Login primero)
    dplyr::group_by(url, user) %>%
    dplyr::mutate(session = cumsum(type == "LoggedIn")) %>%
    dplyr::ungroup()
  events <- events %>%
    dplyr::filter(profile == "alumno")

  ## -------------------------------------------------------------------------
  ##
  ## we use intermediate$last_event, which contains, for this user and url,
  ## the date of the last registered event, and time_spent in the url by the
  ## user at the  date of the last registered event, and percentage achieved
  ## -------------------------------------------------------------------------
  ##
  if (length(intermediate$last_event) > 0 &
      sum(events$session == 0) > 0) {
      interrupted_sessions_last_event <- events %>%
          dplyr::filter(session == 0) %>%
          dplyr::select(user, url, name, email, title, profile, session) %>%
          dplyr::distinct() %>%
          dplyr::left_join(intermediate$last_event) %>%
          dplyr::rename(date = date_last_event) %>%
          dplyr::mutate(type = "LoggedIn")
    ## problema con el tipo de date, que era date_last_event
    events <- dplyr::bind_rows(
      interrupted_sessions_last_event,
      events
    )
    events <- events %>%
      dplyr::group_by(url, user) %>%
      dplyr::mutate(session = cumsum(type == "LoggedIn")) %>%
      dplyr::ungroup()
  }

  ## ------------------------------------------------------------------------------
  ##
  ## ellapsed_time: for each event, time from beginning of session up to event
  ##
  ## ------------------------------------------------------------------------------
  events <- events  %>%
    dplyr::group_by(user, session, url) %>%
    dplyr::mutate(
      ellapsed_time =  as.numeric(date) - as.numeric(date[1L])
    )

  ##
  ## builds a dataframe "sessions". it computes for each session (and user, url)
  ## the duration of the session, and the previous_duration, which is the time spent
  ## by the user in this url, previously to the session.
  ## sessions: user, session, url, duration_session, previous_duration
  ##
  sessions <- events %>%
    dplyr::group_by(user, session,  url) %>%
    dplyr::summarise(
      duration_session =  diff(as.numeric(range(date)))
    )  %>%
    dplyr::group_by(user, url) %>%
    dplyr::mutate(
      previous_duration = cumsum(
        c(0, duration_session)[1:length(duration_session)]
      )
    )
  ##

  if (length(intermediate$last_event) > 0){
    sessions <- sessions %>%
      dplyr::left_join(intermediate$last_event) %>%
      dplyr::mutate(previous_duration = previous_duration +
                      dplyr::if_else(
                 is.na(time_spent),
                 0,
                 time_spent
               )
      ) %>%
      dplyr::select( - date_last_event, - time_spent, - percentage)
  }
  ## "time_spent" by the user in the url up to the current event is computed
  ## It sums the time spent in the url during previous sessions, and the
  events <- events %>%
    dplyr::left_join(sessions)
  events <- events %>%
    dplyr::mutate(
      time_spent = ellapsed_time +
        previous_duration
    )
  dplyr::ungroup(events) %>%
    dplyr::select(
      - session,
      - ellapsed_time,
      - previous_duration
    )

}

## ------------------------------------------------------------------------------
##
## For video analytics
##
## ------------------------------------------------------------------------------

prepare_video_notes <- function(notes.column){
        empty_row <- dplyr::tibble(
        Type = character(),
        Position = character(),
        Duration = character(),
        Offset = character()
    )
    purrr::map(
        notes.column,
        ~ dplyr::bind_rows(empty_row,
                jsonlite::fromJSON(.x)$video) %>%
            dplyr::transmute(
                       action_type = Type,
                       position = as.numeric(Position),
                       duration = as.numeric(Duration),
                       offset = Offset
                   )
    )
}

generate_played <- function(duration, slot_width){
    purrr::map2(
               duration,
               slot_width,
               ~ rep(0, ceiling(.x / .y))
               )
    }
generate_last_position <- function(duration, slot_width){
    purrr::map2(
               duration,
               slot_width,
              ~ .y + seq(0, .x, .y)
              )
}
## Note that slots are seq(0, duration, by = slot_width)
get_slot <- function(position_column, slot_width){
    (position_column %/% slot_width) + 1
}

get_element_slot <- function(list_column, position_column, slot_width){
    purrr::pmap_dbl(
               list(list_column, position_column, slot_width),
               ~ ..1[get_slot(..2, ..3)]
               )
}


update_intermediate_videos <- function(intermediate, new_events, slot_width_value = 2){
    ## intermediate is a list, new__events a dataframe, which has already been manipulated
    ## with prepare_events.
    ## slot_width must be the same for all events associated to a given
    ## video (url, element). Ideally, it should never change.

    if (!(nrow(new_events) > 0 &&
           ("notes" %in% names(new_events)) &&
             sum(grepl('\\"video\\"',
                       new_events$notes,
                       ignore.case = TRUE)) > 0)){
        return(intermediate$videos)
    } else {
        if(is.null(intermediate$videos)) intermediate$videos <- list()
        new_events_video <- new_events %>%
            dplyr::filter(grepl('\\"video\\"', notes, ignore.case = TRUE)) %>%
            dplyr::mutate( notes = prepare_video_notes(notes)) %>%
            tidyr::unnest(notes)
        ## ---------------------------------------------------------------------
        ##
        ## For each video, characterized by url and element, we set the slot_width.
        ## If ath url and element appear in intermediate$videos, then the value of
        ## slot_width should  be respected, otherwise, it it set to the value of
        ## slot_width given aa argument of the function.
        ##
        ## ------------------------------------------------------------------------
        new_slot_widths <-
            new_events_video %>%
            dplyr::select(., url, element) %>%
            dplyr::distinct(.) %>%
            dplyr::mutate(slot_width = slot_width_value)

        if(!is.null(intermediate$videos$slot_width.df)){
            new_slot_widths <- new_slot_widths %>%
                dplyr::anti_join(intermediate$videos$slot_width.df)
        }
        intermediate$videos$slot_width.df <- dplyr::bind_rows(
            intermediate$videos$slot_width.df,
            new_slot_widths
            )
        ## add the slot_width to all new_events_video
        new_events_video <- new_events_video %>%
            dplyr::left_join(intermediate$videos$slot_width.df)
        ## ---------------------------------------------------------------------
        ##
        ## Check user, url, and video( = element) which do not appear in
        ## intermediate and generate incialized vector played and last_position
        ##
        ## ------------------------------------------------------------------------------
        new_videos <- new_events_video %>%
            dplyr::select(., user, url, element, duration, slot_width) %>%
            dplyr::distinct(.) %>%
            dplyr::mutate(
                       played = generate_played(duration, slot_width),
                       last_position = generate_last_position(duration, slot_width)
                   )
        if (!is.null(intermediate$videos$user_summary)){
            new_videos <- new_videos %>%
                dplyr::anti_join(., intermediate$videos$user_summary %>%
                                    dplyr::select(user, url, element))
            }

        ## names(new__videos): user, url, element, duration, slot_width,
        ## played and last_position
        intermediate$videos$user_summary <- dplyr::bind_rows(
                                          intermediate$videos$user_summary,
                                          new_videos
                                      )
        ## we have to update played and last_position
        new_events_video <- new_events_video %>%
            dplyr::mutate(slot = get_slot(position, slot_width))
        new_events_video <- new_events_video %>%
            dplyr::left_join(intermediate$videos$user_summary %>%
                             dplyr::select(-duration, -slot_width)) %>%
            dplyr::mutate(
                       last_position_current = get_element_slot(
                           last_position,
                           position,
                           slot_width
                       ),
                       played_current = get_element_slot(
                           played,
                           position,
                           slot_width
                       )
                   )
        new_events_video <- new_events_video %>%
            dplyr::group_by(user, url, element, slot) %>%
            dplyr::mutate(last_position_updated =  dplyr::lag(
                                                              position,
                                                              1,
                                                              default = head(last_position_current, 1L)
                                                          ),
                          last_position_current =  dplyr::if_else(
                                                              position < last_position_updated,
                                                              position,
                                                              last_position_updated)
                          )
        played_slots <- new_events_video %>%
            dplyr::group_by(user, url, element, slot) %>%
            dplyr::summarise(
                       played_updated = sum(position - last_position_current) +
                           head(played_current, 1L),
                       last_position_updated = tail(position, 1L)
                   ) %>%
            dplyr::group_by(user, url, element) %>%
            tidyr::nest(
                       slot,
                       played_updated,
                       last_position_updated,
                       .key = "played_last_position.df"
                   )

        iv_updated <- intermediate$videos$user_summary %>%
            dplyr::inner_join(played_slots) %>%
            dplyr::mutate(
                       played = purrr::map2(
                                           played,
                                           played_last_position.df,
                                           function(x, y) {
                                               x[y$slot] <- y$played_updated
                                               x
                                           }
                                       ),
                       last_position = purrr::map2(
                                                  last_position,
                                                  played_last_position.df,
                                                  function(x, y) {
                                                      x[y$slot] <- y$last_position_updated
                                                      x
                                                  }
                                              )
                   ) %>%
            dplyr::select(-played_last_position.df)
        intermediate$videos$user_summary <-
            dplyr::bind_rows(
                       intermediate$videos$user_summary%>%
                       dplyr::anti_join(played_slots),
                       iv_updated
                   )
        intermediate$videos
            }
}



## ------------------------------------------------------------------------------
##
## aggregate from intermediatevideo
##
## ------------------------------------------------------------------------------
intermediate2aggregate_videos <- function(intermediate_videos){
    ## assuming intermediate_videos is a list with two df:
    ## slot_width.df, which holds for all url and element the slot width used
    ##and user_summary.
    ## element is assumed to identify the video within the url

    if(is.null(intermediate_videos)) return(NULL)

    generate_slot_width_vector<- function(slot_width, duration){
        slot_vector <- rep(slot_width, duration %/% slot_width)
        if ( ! duration %% slot_width == 0) {
            slot_vector <- c(slot_vector,  duration %% slot_width)
        }
        slot_vector
    }
    intermediate_videos$user_summary$played <-
        purrr::pmap(
                   list(
                       intermediate_videos$user_summary$played,
                       intermediate_videos$user_summary$slot_width,
                       intermediate_videos$user_summary$duration
                   ),
                   ~ round(..1 / generate_slot_width_vector(..2, ..3))
               )

    viz <- intermediate_videos$user_summary %>%
        dplyr::group_by(url, element) %>%
        dplyr::summarise(nusers = n(),
                         viz_vector = list(purrr::reduce(
                                                      played,
                                                      function(x, y) {
                                                          purrr::map2(
                                                                     x,
                                                                     y,
                                                                     ~ c(.x, .y)
                                                                 )
                                                      }
                                                  ))
                         )
    viz <- viz %>%
        dplyr::mutate(
                   total = purrr::map(
                                      viz_vector,
                                      ~ purrr::map_dbl( .x, ~ sum(.x /nusers))
                                  ),
                   frequencies = purrr::map(
                                            viz_vector,
                                            ~ purrr::map(.x, ~ as.list(table(.x)))
                                        )
               )
    viz %>% dplyr::select(-viz_vector)
}

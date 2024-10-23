(ns org.zsxf.test-data.states)

(def schema {:state/abbreviation {:db/unique :db.unique/identity}
             :state/name {:db/unique :db.unique/identity}
             :state/electoral-votes {:db/cardinality :db.cardinality/one}})

(def statoms [{:state/name "Alabama" :state/abbreviation "AL" :state/electoral-votes 9}
              {:state/name "Alaska" :state/abbreviation "AK" :state/electoral-votes 3}
              {:state/name "Arizona" :state/abbreviation "AZ" :state/electoral-votes 11}
              {:state/name "Arkansas" :state/abbreviation "AR" :state/electoral-votes 6}
              {:state/name "California" :state/abbreviation "CA" :state/electoral-votes 54}
              {:state/name "Colorado" :state/abbreviation "CO" :state/electoral-votes 10}
              {:state/name "Connecticut" :state/abbreviation "CT" :state/electoral-votes 7}
              {:state/name "Delaware" :state/abbreviation "DE" :state/electoral-votes 3}
              {:state/name "Florida" :state/abbreviation "FL" :state/electoral-votes 30}
              {:state/name "Georgia" :state/abbreviation "GA" :state/electoral-votes 16}
              {:state/name "Hawaii" :state/abbreviation "HI" :state/electoral-votes 4}
              {:state/name "Idaho" :state/abbreviation "ID" :state/electoral-votes 4}
              {:state/name "Illinois" :state/abbreviation "IL" :state/electoral-votes 19}
              {:state/name "Indiana" :state/abbreviation "IN" :state/electoral-votes 11}
              {:state/name "Iowa" :state/abbreviation "IA" :state/electoral-votes 6}
              {:state/name "Kansas" :state/abbreviation "KS" :state/electoral-votes 6}
              {:state/name "Kentucky" :state/abbreviation "KY" :state/electoral-votes 8}
              {:state/name "Louisiana" :state/abbreviation "LA" :state/electoral-votes 8}
              {:state/name "Maine" :state/abbreviation "ME" :state/electoral-votes 4}
              {:state/name "Maryland" :state/abbreviation "MD" :state/electoral-votes 10}
              {:state/name "Massachusetts" :state/abbreviation "MA" :state/electoral-votes 11}
              {:state/name "Michigan" :state/abbreviation "MI" :state/electoral-votes 15}
              {:state/name "Minnesota" :state/abbreviation "MN" :state/electoral-votes 10}
              {:state/name "Mississippi" :state/abbreviation "MS" :state/electoral-votes 6}
              {:state/name "Missouri" :state/abbreviation "MO" :state/electoral-votes 10}
              {:state/name "Montana" :state/abbreviation "MT" :state/electoral-votes 4}
              {:state/name "Nebraska" :state/abbreviation "NE" :state/electoral-votes 5}
              {:state/name "Nevada" :state/abbreviation "NV" :state/electoral-votes 6}
              {:state/name "New Hampshire" :state/abbreviation "NH" :state/electoral-votes 4}
              {:state/name "New Jersey" :state/abbreviation "NJ" :state/electoral-votes 14}
              {:state/name "New Mexico" :state/abbreviation "NM" :state/electoral-votes 5}
              {:state/name "New York" :state/abbreviation "NY" :state/electoral-votes 28}
              {:state/name "North Carolina" :state/abbreviation "NC" :state/electoral-votes 16}
              {:state/name "North Dakota" :state/abbreviation "ND" :state/electoral-votes 3}
              {:state/name "Ohio" :state/abbreviation "OH" :state/electoral-votes 17}
              {:state/name "Oklahoma" :state/abbreviation "OK" :state/electoral-votes 7}
              {:state/name "Oregon" :state/abbreviation "OR" :state/electoral-votes 8}
              {:state/name "Pennsylvania" :state/abbreviation "PA" :state/electoral-votes 19}
              {:state/name "Rhode Island" :state/abbreviation "RI" :state/electoral-votes 4}
              {:state/name "South Carolina" :state/abbreviation "SC" :state/electoral-votes 9}
              {:state/name "South Dakota" :state/abbreviation "SD" :state/electoral-votes 3}
              {:state/name "Tennessee" :state/abbreviation "TN" :state/electoral-votes 11}
              {:state/name "Texas" :state/abbreviation "TX" :state/electoral-votes 40}
              {:state/name "Utah" :state/abbreviation "UT" :state/electoral-votes 6}
              {:state/name "Vermont" :state/abbreviation "VT" :state/electoral-votes 3}
              {:state/name "Virginia" :state/abbreviation "VA" :state/electoral-votes 13}
              {:state/name "Washington" :state/abbreviation "WA" :state/electoral-votes 12}
              {:state/name "West Virginia" :state/abbreviation "WV" :state/electoral-votes 4}
              {:state/name "Wisconsin" :state/abbreviation "WI" :state/electoral-votes 10}
              {:state/name "Wyoming" :state/abbreviation "WY" :state/electoral-votes 3}])

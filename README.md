# DepressionAnxietyStress
Predicting Depression, Anxiety and Stress

## Data Introduction

Dataset from Predicting Depression, Anxiety, and Stress from Kaggle (https://www.kaggle.com/yamqwe/depression-anxiety-stress-scales/version/13).

Data collected as survey from 2017-2019 to anyone using online-version of Depression Anxiety Stress Scale (DASS). There were 40,000 participants in this 42-question survey and was answered on a 4 point scale to indicate how often the question was applicable to the person in the past week.

Other surveys were also given to the participants to gather background data on their personality and demographics.

## Objective of Project

To get enriching information about the DASS (Depression Anxiety Stress Scales) to find better relationships between variables and make better sense of the results.

Exploration of the data to see if there are any particular personality trait or demographic variable that has a correlation with DASS scores

Apply ML to see if we can predict the DASS scores

## Defining DASS Scores

DASS: Depression Anxiety Stress Scales
The DASS-42 is a 42 item self-report scale designed to measure the emotional states of depression, anxiety and stress
The essential function of the DASS is to assess the severity of the core symptoms of Depression, Anxiety and Stress. High scores on the DASS would certainly alert the clinician to a high level of distress in the patient and this would need to be explored further within the interview process

Each of the 42 questions is scored on a 4-point scale ranging from 0 (“Did not apply to me at all”) to 3 (“Applied to me very much, or most of the time”). Scores for Depression, Anxiety and Stress are calculated by summing the scores for the relevant items:
 Depression: 3, 5, 10, 13, 16, 17, 21, 24, 26, 31, 34, 37, 38, 42 
Anxiety: 2, 4, 7, 9, 15, 19, 20, 23, 25, 28, 30, 36, 40, 41 
Stress: 1, 6, 8, 11, 12, 14, 18, 22, 27, 29, 32, 33, 35, 39



## Defining Personality Scores


The Ten Item Personality Inventory is a ten-item test to measure personality traits and characteristics as conceptualized by the five-factor model. 
Each of the five items was rated on a 7-point scale ranging from 1 (disagree strongly) to 7 (agree strongly).
It assesses the 5 personality traits using two question for each trait:
1. Extraversion  
2. Agreeableness  
3. Conscientiousness  
4. Emotional Stability  
5. Openness  

## Defining Personality Scores

Recode the reverse-scored columns (i.e., recode a 7 with a 1, a 6 with a 2, a 5 with a 3, etc.). The reverse scored items are 2, 4, 6, 8, & 10.
Extraversion: 1, 6R
Agreeableness: 2R, 7
Conscientiousness; 3, 8R
Emotional Stability: 4R, 9
Openness to Experiences: 5, 10R

Example: A participant scores a 5 on item 1 (Extraverted, enthusiastic) and a 2 on item 6(Reserved, quiet)

## Defining Demographics

Generic Demographics Survey was also given to participants
Example of questions include:
"How much education have you completed?", 1=Less than high school, 2=High school, 3=University degree, 4=Graduate degree
"What is your gender?", 1=Male, 2=Female, 3=Other
"How many years old are you?"
"What is your sexual orientation?", 1=Heterosexual, 2=Bisexual, 3=Homosexual, 4=Asexual, 5=Other


## EDA: DASS Severity Distribution

### Distribution of Depression Severity

![Alt text](./Graphs/dep_distr.png?raw=true "Title")

As you can tell in the distribution for depression severity, a high number of participants score in the extremely severe bracket for depression

### Distribution of Anxiety Severity
![Alt text](./Graphs/anx_distr.png?raw=true "Title")

In the distribution for anxiety severity, it is quite similar to the depression severity depression in where there is 
a high number of participants score in the extremely severe bracket


### Distribution of Stress Severity
![Alt text](./Graphs/stress_distr.png?raw=true "Title")

Stress distribution shows a significant number of participants with a more normal levels of stress



## EDA: DASS Severity and Personality Trait Distribution

### Depression Severity and Personality Traits

![Alt text](./Graphs/dep_personality_line.png?raw=true "Title")

As we can see, the more severe the depression, the lower the average scores for all 5 personality traits becomes.

Things to note:
The y-axis is skewed such that it shows toward where the data lies. The personality scores can range between 1 to 7
Can’t figure out why extraversion line will not display on the line graph

The green line which is Emotional Stability trait has the steepest downward slope out of all the other traits. 
One could interpret this as the more depressed you are, the more emotionally-unstable you are.


### Anxiety Severity and Personality Traits

![Alt text](./Graphs/anxiety_personality_line.png?raw=true "Title")

Likewise with anxiety, we can also see that there is a downward trend for average scores for all 5 personality traits .

Again, Emotional Stability score goes down the fastest the more anxious you are.


### Stress Severity and Personality Traits

![Alt text](./Graphs/stress_personality_line.png?raw=true "Title")

Stress is similar to depression and anxiety

Again, Emotional Stability score goes down the fastest the more stressed you are.

## EDA: DASS Scores and Demographics

### DASS Score by Gender 

![Alt text](./Graphs/gender_dass.png?raw=true "Title")

Male has the lowest DASS Scores
Other gender category has the highest DASS scores


### DASS Score by Orientation

![Alt text](./Graphs/orientation_dass.png?raw=true "Title")

Heterosexual have the lowest DASS scores
Bisexual orientation group has the highest DASS scores. Hetersexual orientation group is the lowest and all the other sexual orientation group are also relatively higher compared to hetersexual.

### DASS Score by Age Group

![Alt text](./Graphs/agegroup_dass.png?raw=true "Title")

There seems to be a trend with the DASS scores for the age group. The older the age groups, the lower the DASS scores are

We can see that the below 18 age group has the highest DASS scores compared to the older age group of 35 and above having the lowest score


### DASS Score by Race

![Alt text](./Graphs/race_dass.png?raw=true "Title")

Some race also have lower Dass scores average such as Black, Asian, and white

Other race like Native American and Arab have the higher DASS score average

### DASS Score by Married

![Alt text](./Graphs/married_dass.png?raw=true "Title")
This graph shows that never married people have the highest DASS score average, whereas currently married people have the lowest DASS score

But the DAS increases if something happened, and they are no longer together


### DASS Score by Education Completed

![Alt text](./Graphs/education_dass_scores.png?raw=true "Title")
This graph shows that the higher the education you have, the lower your DASS score are.

However, there seems to be a cap after completing university (undergraduate) degree.
This is interesting because generally acknowledged that the more highly educated someone is, the more money they make.
There have been past studies that have found that happiness plateaus once a person earns $75,000 per year. 


### DASS Score by Religion

![Alt text](./Graphs/religion_dass.png?raw=true "Title")

Some religion have high Dass scores average such as Christian (Morman) 

Other religion like Buddhist and Christian (Protestant) have the lowest DASS scores

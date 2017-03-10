
####pandas sscript to process the data ####

import numpy
from pandas import *
from datetime import datetime
from datetime import date, timedelta




######################################
dfa=pandas.read_csv('part_a.csv') ###### please give the location of the file generated from script A here
dfb=pandas.read_csv('part_b.csv')###### please give the location of the file generated from script B here
dfc = pandas.read_csv('part_c_copy.csv')###### please give the location of the file generated from script C here
df1= pandas.read_csv('rep_1.csv')
df2= pandas.read_csv('rep_2.csv')

df_grouped=dfa.groupby('Uid')
dfb_grouped=dfb.groupby('UserId')
dfb=dfb.rename(columns={'UserId':'Uid'})
    
d1 = datetime.strptime('2015-02-01',"%Y-%m-%d")####please give your start date here as yyyy,mm,dd---to This is the same start date you have given in the SQL program
d2 = datetime.strptime('2015-03-31',"%Y-%m-%d")#### please give your end date here as yyyy,mm,dd------This should always be the  date on which you are running the code,
                                                 #### as that will contain the latest reputation of the user, This is also the same date you would given in the SQL program.

weeks=range(1,dfa['Analysis_Week'].max()+1)





#####please give the date ranges in between where you want the reputation calculated here.#######


k=0
temp=[]

for i in list(dfa['Uid'].unique()): ####creating an artificial  record for a given user id for all analysis weeks for the sake of the joining users and badges tables
    for j in range(1,len(weeks)+1):
        temp.append({'Uid': i, 'Analysis_Week': j})
       
df3=pandas.DataFrame(temp)

df4=pandas.DataFrame(index=None)
df5=pandas.DataFrame(index=None)

for  i in list(dfa['Uid'].unique()):####doing left join of artificial record of user with actual record for users and badges table
    
    df4=df4.append(df3.loc[df3['Uid'].isin([i])].merge(dfa.loc[dfa['Uid'].isin([i])],on='Analysis_Week',how='left'))
    df5=df5.append(df3.loc[df3['Uid'].isin([i])].merge(dfb.loc[dfb['Uid'].isin([i])],on='Analysis_Week',how='left'))

df4=df4.fillna(0)
df5=df5.fillna(0)###removing all the NaN's


df_final= df4.merge(df5,on=['Analysis_Week','Uid_x'],how='inner')###joining the users and badges table
df_final=df_final.drop(['Uid_y_x','Uid_y_y','Week_y','upvotes','downvotes'],axis=1)
df_final=df_final.rename(columns={'Uid_x':'Uid','Week_x':'calendar_week'})


df_final1=pandas.DataFrame(columns=[df_final.columns])
for  i in list(df_final['Uid'].unique()):
    
    tempdf=df_final.loc[df_final['Uid'].isin([i])]
    
    temp=list(tempdf['tenure'])
    
    rlist=list(tempdf['calendar_week'])
    vlist=list(tempdf['views'])
    replist=list(tempdf['Reputation'])
    tempdf['Reputation']= replist[replist.index(filter(lambda x: x!=0, replist)[0])]
    tempdf['views']= vlist[vlist.index(filter(lambda x: x!=0, vlist)[0])]####creating a continuous record for tenure,even for weeksthe user was not present
    
       
    ylist=list(tempdf['Year'])
    
     
    tempdf['Year']= ylist[ylist.index(filter(lambda x: x!=0, ylist)[0])]
    
    ################################
    for l in temp:
        if l !=0:
            i=temp.index(l)
            s=l
            break
    start= s-temp.index(s)
    end= s+(len(temp)-temp.index(s))


    temp= range(int(start),int(end))   
    temp1=[]
    for j in temp:
        if j <=0:
            j=0
            temp1.append(j)

        if j>0:
            temp1.append(j)
        
    ###############################
    for la in rlist:
        if la !=0:
            ia=rlist.index(la)
            sa=la
            break
    starta= sa-rlist.index(sa)
    enda= sa+(len(rlist)-rlist.index(sa))


    rlist= range(int(starta),int(enda))   
    rlist1=[]
    for ja in rlist:
        if ja <=0:
            ja=0
            rlist1.append(ja)

        if ja>0:
            rlist1.append(ja)
    ##################################    
    tempdf['tenure']=temp1
    tempdf['calendar_week']=rlist1
    
    df_final1=df_final1.append(tempdf)


df_final1= df_final1.merge(dfc ,on=['calendar_week','Uid'], how='left')
df_final1=df_final1.fillna(0)

####rearranging the columns in the dataframe
df_final1=df_final1[['Analysis_Week','Answers_this_week','Uid','Year','calendar_week','upvotes','downvotes','tenure','score','views_x','views_y','Reputation','bronzec','silverc','goldc','questionb','answerb'
                     ,'comments','commentspa','viewspa']]


#####calculating the start and end dates ############

###########function to calculate start and end dates##################
def week(year, week):
    ret = datetime.strptime('%04d-%02d-1' % (year, week), '%Y-%W-%w')
    if date(year, 1, 4).isoweekday() > 4:
        ret -= timedelta(days=7)
    return ret-timedelta(days=1)
    
################################################################

dd1 = d1
dd2 = d2
c=0
dates_start1 = []
dates_end1 = []
while True:
    if c!=0 and datetime.strptime(dates_end1[-1], ("%Y-%m-%d %H:%M:%S")) + timedelta(days=1) + timedelta(days = 7)>dd2:
        dates_start1.append(str( datetime.strptime(dates_end1[-1], ("%Y-%m-%d %H:%M:%S")) + timedelta(days=1)))
        dates_end1.append(str(dd2))
        break
    day_of_week = dd1.isocalendar()[2]
    #print day_of_week
    if day_of_week == 7:
        dates_start1.append(str(dd1))
        dates_end1.append(str(dd1 + timedelta(days = 6)))
        
    else:
        if c==0:
            dates_start1.append(str(dd1))
        else:
            dates_start1.append(str(end + timedelta(days =1)))
        dates_end1.append(str(dd1 + timedelta(days = (6 - dd1.isocalendar()[2]))))
        end = dd1 + timedelta(days = (6 - dd1.isocalendar()[2]))
        #print dates_start1[-1]
        #print dates_end1[-1]
    dd1 = dd1 + timedelta(days = 7)
    c+=1
print dates_start1
print dates_end1

dates_start = dates_start1
dates_end = dates_end1




#####joining user repuatation ########

df=df1.merge(df2,on=['Uid','week'],how='outer')

df=df.fillna(0)
df=df.sort(['Uid','week'])
df=df.rename(columns = {'week':'calendar_week','views_y':'views'})

df_final1=df_final1.merge(df,on=['Uid','calendar_week'],how='left')
df_final1=df_final1.fillna(0)
df_final1= df_final1.drop(['downvotes_user_got','downvotes_by_user'],axis=1
                          )
df_final1=df_final1.rename(columns = {'views_y':'viewc','views_x':'views'})
df_final1['cumiliative_rep']=''

df_final1['Reputation_gain_per_week']=''

df_final1['backdated_rep']=''

#####finding how much reputation the user has gained every week######
df_final2=pandas.DataFrame(columns=df_final1.columns)
for  i in list(df_final1['Uid'].unique()):

    tempdf=df_final1.loc[df_final1['Uid'].isin([i])]
    list1=[]
    for j, r in tempdf.iterrows():
        list1.append(5*r['upvotes']+(-1*r['downvotes'])+r['question_points']+r['Answer_points']+r['points'])
    
    
    cumsum=pandas.Series(numpy.cumsum(list1),index=tempdf.index)
    tempdf['cumiliative_rep']=cumsum
    tempdf['Reputation_gain_per_week']=cumsum.diff()
    

    templist= [0]*len(list1)
    templist1=tempdf['Reputation_gain_per_week'].fillna(0).tolist()
    
    templist[len(templist)-1]=tempdf.iloc[-1]['Reputation']
    templist[len(templist)-2]=templist[len(templist)-1]-templist1[len(templist1)-1]
    for i in range(-3,-(len(templist))-1,-1):
        templist[i]=templist[i+1]-templist1[i+1]
    
    tempdf['backdated_rep']=pandas.Series(templist,index=tempdf.index)
    
    df_final2=df_final2.append(tempdf)

df_final2=df_final2.fillna(0)

####renaming and re ordering the columns#####
df_final2=df_final2.rename(columns = {'Analysis_Week':'week','Answers_this_week':'answers',
                                      'Uid':'uid','score':'scores','questionb':'questionbc','Reputation':'rep','answerb':'answerbc','comments':'commentc','cumiliative_rep':'totrep','Reputation_gain_per_week':'repgain'})


df_final2=df_final2[['uid','week','calendar_week','answers','scores','upvotes','downvotes','views','rep','backdated_rep','totrep','tenure','goldc',
                     'silverc','bronzec','answerbc','questionbc','commentc','commentspa','viewc','viewspa','repgain','question_points','Answer_points','Acceptedanswers','points']]

#df_final2= df_final2.loc[(df_final2['week']>=float(co_week_1)) & (df_final2['week']<=float(co_week_2))] ###please give the filter condition here





df_final2=df_final2.drop(['totrep'],axis=1)
df_final2=df_final2.rename(columns={'backdated_rep':'totrep'})



df_final2=df_final2[['uid','week','calendar_week','answers','scores','upvotes','downvotes','views','rep','totrep','tenure','goldc',
                     'silverc','bronzec','answerbc','questionbc','commentc','commentspa','viewc','viewspa','repgain','question_points','Answer_points','Acceptedanswers','points']]


df_final2.repgain= df_final2.repgain.shift(-1)
df_final2=df_final2.fillna(0)


########Adding the start dates and end dates###############


df_final2['from_date']=''
df_final2['to_date']=''

df_final3=pandas.DataFrame(columns=df_final2.columns)

for  i in list(df_final2['uid'].unique()):

    tempdf=df_final2.loc[df_final2['uid'].isin([i])]
    tempdf['from_date']=pandas.Series(dates_start,index=tempdf.index)
    tempdf['to_date']=pandas.Series(dates_end,index=tempdf.index)
    tempdf['week']=pandas.Series(range(1,len(tempdf['week'])+1),index=tempdf.index)

    df_final3=df_final3.append(tempdf)


df_final3=df_final3[['uid','week','calendar_week','from_date','to_date','answers','scores','upvotes','downvotes','views','rep','totrep','tenure','goldc',
                     'silverc','bronzec','answerbc','questionbc','commentc','commentspa','viewc','viewspa','repgain','question_points','Answer_points','Acceptedanswers','points']]

#####writing to a csv file#########
df_final3.to_csv('final_data_with_user_rep_2.csv',index=False)###writing into a csv file. This is the final file


















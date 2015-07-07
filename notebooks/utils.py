import os
import sys
import hashlib
import datetime


def random_subset(group):
    salt, n_groups, selected = group.split(':', 2)
    n_groups = int(n_groups)
    selected = map(int, selected.split(','))
    for i in selected:
        assert 0 <= i < n_groups
    
    return (
        lambda kv: (int(hashlib.sha1(salt + str(kv[0])).hexdigest()[:6], 16) % n_groups) in selected
    )


def load_sessions(sc):
    train_sessions = sc.pickleFile('yoochoose/train_sessions.pickle', 400)
    test_sessions = sc.pickleFile('yoochoose/test_sessions.pickle', 400)
    return train_sessions, test_sessions


def positive_session((session_id, (clicks, buys))):
    return len(buys) > 0


def construct_feature_set(sc, name, splits, feature_extractor, **kwargs):
    os.system('mkdir -p features/%s' % name)
    
    # feature description
    sample_sessions = splits.values()[0]
    _, _, _, features = sample_sessions.flatMap(lambda session: feature_extractor(session, **kwargs)).first()
    with open('features/%s/features.fd' % name, 'w') as f:
        for i, (typ, val) in enumerate(features):
            f.write('%d\t%s\n' % (i, typ))
        
    # feature files
    print 'Constructing feature sets:', name
    start_time = datetime.datetime.now()
    for split_name, sessions in splits.iteritems():
        print '   %s' % split_name
        sys.stdout.flush()
        hdfs_filename = 'yoochoose/%s_%s' % (name, split_name)
        sessions\
            .flatMap(lambda session: feature_extractor(session, **kwargs))\
            .map(lambda (session_id, item_id, target, features): '%d\t%d\t%s\t0\t' % (session_id, target, item_id) + 
                 '\t'.join(str(val) for _, val in features))\
            .saveAsTextFile(hdfs_filename)
    print '   %d min' % int((datetime.datetime.now() - start_time).total_seconds() / 60)


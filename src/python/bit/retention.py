#-*-coding:utf-8-*-
"""
@package bit.retention
@brief Implements a generic retention policy

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['RetentionPolicy']

from .utility import (  seconds_to_datetime,
                        delta_to_seconds )
from bisect import bisect_left
from butility import frequncy_to_seconds


class RetentionPolicy(object):
    """A policy defined by a string that defines a retention policy.

    Each retention period of frequency:history is separated by a comma.
    Frequencies and histories are specified using the following suffixes:

    - s - second
    - h - hour
    - d - day
    - m - month
    - y - year

    There can be a prefix per retention period, x:, which indicates the amount of samples to keep at the starting 
    point of the period.
    For instance, 5:1h:1d,2d:4w, means it will keep the most recent 5 samples in the first period, no matter what.

    If you specify x-<oolicy> you indicate that the first x samples will be ignored entirely and just remain.
    This is useful if you want to assure that the most recent sample will always remain for instance.

    Example
    =======

    - 10s:14d
     + One sample every 10s for 14days
    - 1h:1d,1d:14d,14d:28d,30d:1y
     + 24 samples for a day, then a sample per day for 14 days, then 2 14d for a month, and monthlies for a year
    """
    __slots__ = (
                    # our retention rules, as list(tuple(keep, frequency_in_s, duration_in_seconds), ...)
                    '_rules',
                    # A counter for the amount of most-recent samples to keep
                    '_keep_initial'
                )


    def __init__(self, policy_string):
        """Initialize the instance with a policy string
        @param policy_string format: frequency:history, where frequency and history are <number><unit> pairs, 
        like 13s, or 14d"""
        self._rules, self._keep_initial = self._parse_policy(policy_string)

    # -------------------------
    ## @name Utilities
    # @{
    
    def _parse_policy(self, policy):
        """@return a rules structure (see `_rules` member), followed by the amount of 
        @param policy a string identifying the policy
        @throw ValueError if the string could not be parsed or if the rules didn't make sense"""
        keep_initial = 0
        tokens = policy.split('-')
        if len(tokens) == 2:
            try:
                keep_initial = int(tokens[0])
            except ValueError:
                raise ValueError("Could not parse global keep value '%s'" % tokens[0])
            # end handle exception
            policy = tokens[1]
            if not policy:
                return list(), keep_initial
            # end allow no rules
        # end parse keep_initial

        rules = list()
        f_to_s = frequncy_to_seconds
        for period in policy.split(','):
            tokens = period.strip().split(':')
            if len(tokens) not in (2,3):
                raise ValueError("Policy '%s' was malformed, should be '[keep:]frequency:duration'" % period)
            # end

            keep = 0
            if len(tokens) == 3:
                try:
                    keep = int(tokens[0])
                except ValueError:
                    raise ValueError("'keep' portion of period '%s' must be an integer" % period)
                # end handle exceptions
                tokens = tokens[1:]
            # end handle keep

            frequency, duration = tokens
            frequency, duration = f_to_s(frequency), f_to_s(duration)
            if duration / frequency < 1:
                raise ValueError('Frequency cannot be larger than the duration')
            # end handle boundary condition

            rules.append((keep, frequency, duration))
            if len(rules) > 1:
                frequency = rules[-1][1]
                prev_frequency = rules[-2][1]
                if prev_frequency > frequency:
                    raise ValueError("Frequency must not get less granular in following retention periods")
                # end check frequency
        # end for each period
        return rules, keep_initial

    ## -- End Utilities -- @}

    # -------------------------
    ## @name Interface
    # @{
    
    def filter(self, now, samples, ordered=False):
        """Filter the given samples by the policy we were initialized with
        @param now seconds since epoch specifying the current time
        @param samples iterable of tuples of (datetime, data). The only relevant field is the datetime object.
        It must be sorted ascending, from oldest to newest (native to what we have in the datbase)
        @param ordered if False, samples will be assumed to be unordered, and thus ordered before we begin.
        Set this True if your input data is already ordered from most recent to oldest sample
        @return tuple(new_samples, removed_samples) of a new sample list with all removed_samples removed.
        The order was reversed, such that a newer samples are before older samples
        
        @note The algorithm works like a prune, as such it will start dropping samples if there are too many 
        in the period it looks at. Samples that a furthest away from their ideal position will be dropped before
        those that are closer to it."""
        if not ordered:
            samples = sorted(samples, key=lambda k: k[0])
        # end handle ordering

        # Samples are from old --> new
        ns = list()     # new samples
        fs = list()     # future samples (those that are in the future)
        ds = list()     # dropped samples

        assert not hasattr(samples, 'next'), "cannot work with iterators"

        lr = len(self._rules)
        ls = len(samples)
        to_time = now
        sid = 0               # sample id

        # For now, we natively work from new to old, just because our retention is sorted that way
        # TODO: Save that time of reversal, and make it go based on the drop-off date, instead of 'now'
        samples = list(reversed(samples))
        # Samples are from new --> old  (like our rules)

        # Handle special case: we have no rule, but a keep value
        if self._keep_initial and not self._rules:
            return samples[:self._keep_initial], samples[self._keep_initial:]
        # end early bail-out

        for rid, (keep, frequency, retention_span) in enumerate(self._rules):
            # Compute the boundary, in datetimes
            from_time = to_time - retention_span

            # from_date --> to_date ---> now
            from_date = seconds_to_datetime(from_time)
            to_date = seconds_to_datetime(to_time)
            assert to_time > from_time
            assert to_date > from_date
            

            in_last_rule = rid + 1 == lr

            # Samples within this retention period
            retention_samples = list()

            # FIND SAMPLES IN RETENTION RANGE
            #################################
            for sid in xrange(sid, ls):
                sample = samples[sid]
                date = sample[0]

                # Is the sample newer ? This only happens if they are in the future compared to now.
                # As we pop samples, we would not see it here either if it wasn't the first time we see it
                if to_date < date or self._keep_initial > 0:
                    fs.append(sample)
                    self._keep_initial -= 1
                    continue
                # end handle older samples
                
                if date <= from_date:
                    # If we are not in the last rule, from_date is to_date in the next
                    # We must be sure a sample is only considered part of the nest boundary
                    break

                    # NOTE: if this is the last rule, the sample could also be ON the boundary but still be 
                    # considered inside as there is no other rule to come and possibly keep it.
                    # Thanks to our special rule to try to keep the last retention period full with samples, even if
                    # they are outside of it (i.e. older  samples), we don't have to make it a special case
                # end don't process samples in other rules

                # sample is on to_date, or behind it, within our valid range
                retention_samples.append(sample)

                # on the last sample, we have to increment ourselves to prevent future iterations
                if sid + 1 == ls:
                    sid += 1
                # end handle border case
            # end for each sample to handle

            if keep:
                ns.extend(retention_samples[:keep])
                del(retention_samples[:keep])
            # end handle keep

            # PRUNE RETENTION SAMPLES
            ##########################
            # If we don't have enough samples, we don't do anything (we cannot rearrange sample position).
            # Otherwise we drop samples that are furthest off their optimal position, from new to old
            # We therefore favor old samples
            num_samples_in_retention_span = retention_span / frequency
            num_samples_to_remove = len(retention_samples) - num_samples_in_retention_span
            if num_samples_to_remove > 0:
                # Build the raster map
                # The lut is sorted from old to new dates
                raster_lut = list()
                for step in xrange(num_samples_in_retention_span):
                    raster_lut.insert(0, seconds_to_datetime(to_time - step * frequency))
                # end for each frequency step
                # Add boundary ! Otherwise our calculation can go out of bounds
                raster_lut.insert(0, from_date)

                # per grid position, keep a list of (distance, sample) tuples for later sorting
                raster_lut_map = dict()

                # Build a map, associating samples with their closest perfect sample, and keep their distance 
                # for later comparison. We only compare to the closest sample to our left, there should never
                # be ambiguous samples as they only ever approach from one side.
                # The last one we always want to keep, so it will not take part in the Russian roulette
                for rsid in xrange(len(retention_samples)):
                    sample = retention_samples[rsid]
                    sample_date = sample[0]

                    closest_raster_index = bisect_left(raster_lut, sample_date)
                    raster_date = raster_lut[closest_raster_index]
                    assert raster_date >= sample_date

                    distance_list = raster_lut_map.setdefault(raster_date, list())
                    distance_list.append((delta_to_seconds(to_date - sample_date), sample))
                # end for each sample to consider

                # Sort every cluster point's samples by distance, and keep only the closest one
                # We must retain the order, which is new to old, to put new ones onto the list first
                # NOTE: our samples are newest to oldest, the lut HAD to be ascending. Therefore we inverse 
                # it to keep the samples in the right order
                for raster_date in reversed(raster_lut):
                    if num_samples_to_remove == 0:
                        break
                    if raster_date not in raster_lut_map:
                        continue
                    distance_list = sorted(raster_lut_map[raster_date], key=lambda k: k[0])

                    for distance, sample in distance_list[1:]:
                        retention_samples.remove(sample)
                        ds.append(sample)
                        num_samples_to_remove -= 1
                        if num_samples_to_remove == 0:
                            break
                        # early abort
                    # end for each sample
                # end for each distance list

                # Only remove as many samples as we have, even if they might be clumped up. That way,
                # Samples can move through the field and distribute themselves more evenly
                assert num_samples_to_remove == 0, 'should have removed all samples - not doing so means something went wrong'

            elif in_last_rule and num_samples_to_remove < 0:
                # We are last, and don't have enough samples
                # If we are the last one, we have no benefit in the letting samples drop of, as it would mean
                # We don't use our retention span properly. We have been allocated for so and so much, and should 
                # use that space even if it means we keep samples that would otherwise be dropped
                num_samples_to_keep = abs(num_samples_to_remove)
                for sid in xrange(sid, sid + min(ls - sid, num_samples_to_keep)):
                    retention_samples.append(samples[sid])
                # end for each samples
                # Make sure the last one we handled will not be handled again
                sid += 1
            # end remove or re-add samples

            # Reset cursor to next retention span
            to_time = from_time

            # all (remaining) retention samples are valid
            ns.extend(retention_samples)
        # end for each frequency/duration in rules

        # Samples that were in the future, must be put at the right spot - the array was inversed in the process
        # So we have to inverse them as well
        if fs:
            fs.extend(ns)
            ns = fs
        # end handle future samples

        # Samples that have not been processed are to be removed
        # If we end up here, sid is the last ID that was processed throughout all rules
        for sid in xrange(sid, ls):
            ds.append(samples[sid])
        # end for each sample

        return ns, ds

    def rules(self):
        """@return a list of triplets of rules we are using. The first entry is the amount of samples to keep in any way,
        the second entry is the frequency, the third is the duration for which to hold it, both values are in
         seconds since epoch"""
        return self._rules

    def keep(self):
        """@return the amount of initial samples that are always to be kept"""
        return self._keep_initial
        

    def num_rule_samples(self, period = None):
        """@return tuple of total amount of samples we allow based on our rule-set AND intermediate_samples.
        Intermediate samples is 0 if period is not given
        @param period if set, it is interpreted as a single period (format like 1d, 14d) indicating the period in 
        which a filter operation will happen"""
        if not self._rules and self._keep_initial:
            return self._keep_initial, 0
        # end handle special case were we just keep samples, without a policy
        # Otherwise, even if global keep is not 0, it's difficult to properly calculate I think

        sc  = 0      # sample count
        isc = 0      # intermediate sample count
        as_start = as_end = 0  # absolute start and end of span

        if period:
            period = frequncy_to_seconds(period)
        # end convert period

        for keep, freq, span in self._rules:
            ssc = span / freq
            sc += ssc
            as_end = as_start + span

            if as_start < period:
                isc += ((min(as_end, period) - as_start) / float(span)) * ssc
            # end handle period

            as_start = as_end
        # end for each span

        return sc, int(isc)

    ## -- End Interface -- @}

# end class RetentionPolicy

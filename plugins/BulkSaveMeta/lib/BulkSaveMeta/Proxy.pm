package BulkSaveMeta::Proxy;

use strict;
use warnings;

no warnings 'redefine';

require MT::Meta::Proxy;
*MT::Meta::Proxy::save = sub {
    return save( @_ );
};

use MT::Serialize;
my $serializer = MT::Serialize->new('MT');

sub save {
    my $proxy = shift;

 # perl funkiness ... keys %{ $proxy->{__objects} } will automatically clobber
 # empty hash reference on that key!
    return unless $proxy->{__objects};
    my @cols = qw( type vchar vchar_idx vdatetime
                   vdatetime_idx vinteger vinteger_idx vfloat
                   vfloat_idx vblob vclob );
    my $od = lc( MT->config( 'ObjectDriver' ) );
    my $bulk_insert = MT->config( 'BulkInsertMeta' );
    unless ( $od =~ /mysql/ && $bulk_insert ) {
        $bulk_insert = undef;
    }
    if ( MT->request( 'meta-save-retry' ) ) {
        # retry
        $bulk_insert = undef;
    }
    my $ds;
    my $i = 0;
    my @col_names;
    my @col_values;
    my @update_cols;
    my $obj_id;
    my $dbd;
    my $replace;
    my $count_field;
    my $driver = MT::Object->driver;
    my $dbh = $driver->{ fallback }->{ dbh };
    foreach my $field ( keys %{ $proxy->{__objects} } ) {
        next if $field eq '';
        next unless $proxy->is_changed($field);
        my $meta_obj = $proxy->{__objects}->{$field};
        ## primary key from core object
        foreach my $pkey ( keys %{ $proxy->{__pkeys} } ) {
            next if ( $pkey eq 'type' );
            my $pval = $proxy->{__pkeys}->{$pkey};
            $meta_obj->$pkey($pval);
        }

        my $pkg = $proxy->{pkg};
        my $meta = $proxy->META_CLASS()->metadata_by_name( $pkg, $field )
            or next; # XXX: Carp::croak("Metadata $field on $pkg not found.");

        my $type = $meta->{type};

        my $meta_col_def = $meta_obj->column_def($type);
        my $meta_is_blob
            = $meta_col_def ? $meta_col_def->{type} eq 'blob' : 0;
        my $enc = MT->config->PublishCharset || 'UTF-8';
        my ( $data, $utf8_data );
        $data = $utf8_data = $meta_obj->$type;
        $dbd = $meta_obj->driver->dbd;
        unless ( ref $data ) {
            $data = Encode::encode( $enc, $data )
                if Encode::is_utf8($data) && $dbd->need_encode;
        }

        if (!$ds) {
            $ds = $meta->{pkg};
            $ds = $ds->datasource;
        }
        my $column_values = $meta_obj->column_values;
        my @_col_values;
        if ( $bulk_insert ) {
            for my $col( @cols ) {
                my $col_name = $ds . '_meta_' . $col;
                if (! $i ) {
                    push @col_names, $col_name;
                    push @update_cols, "${col_name}=VALUES(${col_name})";
                }
                my ( $data, $utf8_data );
                $data = $column_values->{ $col };
                if ( $col eq 'vblob' ) {
                    if ( ref $data ) {
                        $data = 'BIN:' . $serializer->serialize( \$data );
                    } elsif ( defined $data ) {
                        $data = 'ASC:' . $data;
                    } else {
                        $data = undef;
                    }
                }
                $data = $dbh->quote( $data );
                push @_col_values, $data;
            }
            $obj_id = $column_values->{ $ds . '_id' };
            if (! $i ) {
                push @col_names, $ds . '_meta_' . $ds . '_id';
                $count_field = $meta_obj->count({ $ds . '_id' => $obj_id });
            }
            push @_col_values, $obj_id;
            my $c_values = '';
            for my $v ( @_col_values ) {
                if ( $c_values ) {
                    $c_values .= ',';
                }
                if ( defined $v ) {
                    $c_values .= $v;
                } else {
                    $c_values .= 'NULL';
                }
            }
            $c_values = "(${c_values})";
            push @col_values, $c_values;
            # push @col_values, '(' . join(',',@_col_values) . ')';
        }
        $i++;

        $meta_obj->$type( $data, { no_changed_flag => 1 } );

        ## xxx can be a hook?
        if ( !defined $meta_obj->$type() ) {
            $meta_obj->remove;
        }
        else {
            MT::Meta::Proxy::serialize_blob( $field, $meta_obj ) if $meta_is_blob;
            my $meta_class = $proxy->META_CLASS();
            if (! $bulk_insert ) {
                {
                    no strict 'refs';
                    if ( ${"${meta_class}::REPLACE_ENABLED"} ) {
                        $meta_obj->replace;
                    }
                    else {
                        $meta_obj->save;
                    }
                }
            }
            MT::Meta::Proxy::unserialize_blob($meta_obj) if $meta_is_blob;
        }
        unless ( ref $utf8_data ) {
            $meta_obj->$type( $utf8_data, { no_changed_flag => 1 } );
        }
    }
    if ( $bulk_insert ) {
        if ( @col_values ) {
            my ( $driver, $dbh, $sth, $do );
            $driver = MT::Object->driver;
            my $query = 'INSERT INTO mt_' . $ds . '_meta (' . join(',',@col_names) . ') values ';
            $query .= join(',',@col_values);
            if ( $count_field ) {
                $query .= 'ON DUPLICATE KEY UPDATE ' . join(',',@update_cols);
            }
            if ( MT->config( 'DebugMode' ) && MT->config( 'DebugMode' ) == 10 ) {
                MT->log( $query );
            }
            $dbh = $driver->{ fallback }->{ dbh };
            $sth = $dbh->prepare( $query );
            if ( $dbh->errstr ) {
                MT->log( MT->translate( "Error in query: " . $dbh->errstr ) );
                MT->request( 'meta-save-retry', 1 );
                save( @_ );
            }
            $do = $sth->execute();
        }
    }
}

1;
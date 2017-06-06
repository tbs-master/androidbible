package yuku.alkitab.base.storage;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteStatement;
import android.provider.BaseColumns;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.util.Log;
import android.util.Pair;
import com.google.gson.reflect.TypeToken;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import yuku.afw.D;
import yuku.afw.storage.Preferences;
import yuku.alkitab.base.App;
import yuku.alkitab.base.U;
import yuku.alkitab.base.ac.DevotionActivity;
import yuku.alkitab.base.ac.MarkerListActivity;
import yuku.alkitab.base.devotion.ArticleMeidA;
import yuku.alkitab.base.devotion.ArticleMorningEveningEnglish;
import yuku.alkitab.base.devotion.ArticleRefheart;
import yuku.alkitab.base.devotion.ArticleRenunganHarian;
import yuku.alkitab.base.devotion.ArticleRoc;
import yuku.alkitab.base.devotion.ArticleSantapanHarian;
import yuku.alkitab.base.devotion.DevotionArticle;
import yuku.alkitab.base.model.MVersion;
import yuku.alkitab.base.model.MVersionDb;
import yuku.alkitab.base.model.MVersionInternal;
import yuku.alkitab.base.model.ReadingPlan;
import yuku.alkitab.base.model.SyncLog;
import yuku.alkitab.base.model.SyncShadow;
import yuku.alkitab.base.sync.Sync;
import yuku.alkitab.base.sync.SyncAdapter;
import yuku.alkitab.base.sync.SyncRecorder;
import yuku.alkitab.base.sync.Sync_Mabel;
import yuku.alkitab.base.sync.Sync_Pins;
import yuku.alkitab.base.sync.Sync_Rp;
import yuku.alkitab.base.util.Highlights;
import yuku.alkitab.base.util.Sqlitil;
import yuku.alkitab.debug.BuildConfig;
import yuku.alkitab.model.Label;
import yuku.alkitab.model.Marker;
import yuku.alkitab.model.Marker_Label;
import yuku.alkitab.model.ProgressMark;
import yuku.alkitab.model.ProgressMarkHistory;
import yuku.alkitab.util.Ari;
import yuku.alkitab.util.IntArrayList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static yuku.alkitab.base.util.Literals.Array;
import static yuku.alkitab.base.util.Literals.ToStringArray;

public class InternalDb {
	public static final String TAG = InternalDb.class.getSimpleName();

	private final InternalDbHelper helper;

	public InternalDb(InternalDbHelper helper) {
		this.helper = helper;
	}

	/**
	 * _id is not stored
	 */
	private static ContentValues markerToContentValues(final Marker marker) {
		final ContentValues res = new ContentValues();

		res.put(Db.Marker.ari, marker.ari);
		res.put(Db.Marker.gid, marker.gid);
		res.put(Db.Marker.kind, marker.kind.code);
		res.put(Db.Marker.caption, marker.caption);
		res.put(Db.Marker.verseCount, marker.verseCount);
		res.put(Db.Marker.createTime, Sqlitil.toInt(marker.createTime));
		res.put(Db.Marker.modifyTime, Sqlitil.toInt(marker.modifyTime));

		return res;
	}

	public static Marker markerFromCursor(Cursor cursor) {
		final Marker res = Marker.createEmptyMarker();

		res._id = cursor.getLong(cursor.getColumnIndexOrThrow("_id"));
		res.gid = cursor.getString(cursor.getColumnIndexOrThrow(Db.Marker.gid));
		res.ari = cursor.getInt(cursor.getColumnIndexOrThrow(Db.Marker.ari));
		res.kind = Marker.Kind.fromCode(cursor.getInt(cursor.getColumnIndexOrThrow(Db.Marker.kind)));
		res.caption = cursor.getString(cursor.getColumnIndexOrThrow(Db.Marker.caption));
		res.verseCount = cursor.getInt(cursor.getColumnIndexOrThrow(Db.Marker.verseCount));
		res.createTime = Sqlitil.toDate(cursor.getInt(cursor.getColumnIndexOrThrow(Db.Marker.createTime)));
		res.modifyTime = Sqlitil.toDate(cursor.getInt(cursor.getColumnIndexOrThrow(Db.Marker.modifyTime)));

		return res;
	}

	private static Marker_Label marker_LabelFromCursor(Cursor cursor) {
		final Marker_Label res = Marker_Label.createEmptyMarker_Label();

		res._id = cursor.getLong(cursor.getColumnIndexOrThrow("_id"));
		res.gid = cursor.getString(cursor.getColumnIndexOrThrow(Db.Marker_Label.gid));
		res.marker_gid = cursor.getString(cursor.getColumnIndexOrThrow(Db.Marker_Label.marker_gid));
		res.label_gid = cursor.getString(cursor.getColumnIndexOrThrow(Db.Marker_Label.label_gid));

		return res;
	}

	public Marker getMarkerById(long _id) {
		Cursor cursor = helper.getReadableDatabase().query(
			Db.TABLE_Marker,
			null,
			"_id=?",
			new String[]{String.valueOf(_id)},
			null, null, null
		);

		try {
			if (!cursor.moveToNext()) return null;
			return markerFromCursor(cursor);
		} finally {
			cursor.close();
		}
	}

	@Nullable public Marker getMarkerByGid(@NonNull final String gid) {
		final Cursor cursor = helper.getReadableDatabase().query(Db.TABLE_Marker, null, Db.Marker.gid + "=?", Array(gid), null, null, null);

		try {
			if (!cursor.moveToNext()) return null;
			return markerFromCursor(cursor);
		} finally {
			cursor.close();
		}
	}

	/**
	 * Ordered by modified time, the newest is first.
	 */
	public List<Marker> listMarkersForAriKind(final int ari, final Marker.Kind kind) {
		final SQLiteDatabase db = helper.getReadableDatabase();
		final Cursor c = db.query(Db.TABLE_Marker, null, Db.Marker.ari + "=? and " + Db.Marker.kind + "=?", ToStringArray(ari, kind.code), null, null, Db.Marker.modifyTime + " desc", null);
		try {
			final List<Marker> res = new ArrayList<>();
			while (c.moveToNext()) {
				res.add(markerFromCursor(c));
			}
			return res;
		} finally {
			c.close();
		}
	}

	/**
	 * Insert a new marker or update an existing marker.
	 * @param marker if the _id is 0, this marker will be inserted. Otherwise, updated.
	 */
	public void insertOrUpdateMarker(@NonNull final Marker marker) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		if (marker._id != 0) {
			db.update(Db.TABLE_Marker, markerToContentValues(marker), "_id=?", Array(String.valueOf(marker._id)));
		} else {
			marker._id = db.insert(Db.TABLE_Marker, null, markerToContentValues(marker));
		}
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
	}

	public Marker insertMarker(int ari, Marker.Kind kind, String caption, int verseCount, Date createTime, Date modifyTime) {
		final Marker res = Marker.createNewMarker(ari, kind, caption, verseCount, createTime, modifyTime);
		final SQLiteDatabase db = helper.getWritableDatabase();

		res._id = db.insert(Db.TABLE_Marker, null, markerToContentValues(res));
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);

		return res;
	}

	/** Used in migration from v3 */
	public static long insertMarker(final SQLiteDatabase db, final Marker marker) {
		marker._id = db.insert(Db.TABLE_Marker, null, markerToContentValues(marker));
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);

		return marker._id;
	}

	public void deleteMarkerById(long _id) {
		final Marker marker = getMarkerById(_id);

		final SQLiteDatabase db = helper.getWritableDatabase();
		db.beginTransactionNonExclusive();
		try {
			db.delete(Db.TABLE_Marker_Label, Db.Marker_Label.marker_gid + "=?", new String[]{marker.gid});
			db.delete(Db.TABLE_Marker, "_id=?", new String[]{String.valueOf(_id)});
			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
	}

	public void deleteNonBookmarkMarkerById(long _id) {
		SQLiteDatabase db = helper.getWritableDatabase();
		db.delete(Db.TABLE_Marker, "_id=?", new String[]{String.valueOf(_id)});
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
	}

	public List<Marker> listMarkers(Marker.Kind kind, long label_id, String sortColumn, boolean sortAscending) {
		final SQLiteDatabase db = helper.getReadableDatabase();
		final String sortClause = sortColumn + (Db.Marker.caption.equals(sortColumn)? " collate NOCASE ": "") + (sortAscending? " asc": " desc");

		final List<Marker> res = new ArrayList<>();
		final Cursor c;
		if (label_id == 0) { // no restrictions
			c = db.query(Db.TABLE_Marker, null, Db.Marker.kind + "=?", new String[]{String.valueOf(kind.code)}, null, null, sortClause);
		} else if (label_id == MarkerListActivity.LABELID_noLabel) { // only without label
			c = db.rawQuery("select " + Db.TABLE_Marker + ".* from " + Db.TABLE_Marker + " where " + Db.TABLE_Marker + "." + Db.Marker.kind + "=? and " + Db.TABLE_Marker + "." + Db.Marker.gid + " not in (select distinct " + Db.Marker_Label.marker_gid + " from " + Db.TABLE_Marker_Label + ") order by " + Db.TABLE_Marker + "." + sortClause, new String[] {String.valueOf(kind.code)});
		} else { // filter by label_id
			final Label label = getLabelById(label_id);
			c = db.rawQuery("select " + Db.TABLE_Marker + ".* from " + Db.TABLE_Marker + ", " + Db.TABLE_Marker_Label + " where " + Db.Marker.kind + "=? and " + Db.TABLE_Marker + "." + Db.Marker.gid + " = " + Db.TABLE_Marker_Label + "." + Db.Marker_Label.marker_gid + " and " + Db.TABLE_Marker_Label + "." + Db.Marker_Label.label_gid + "=? order by " + Db.TABLE_Marker + "." + sortClause, new String[]{String.valueOf(kind.code), label.gid});
		}

		try {
			while (c.moveToNext()) {
				res.add(markerFromCursor(c));
			}
		} finally {
			c.close();
		}

		return res;
	}

	public List<Marker> listAllMarkers() {
		final SQLiteDatabase db = helper.getReadableDatabase();
		final Cursor c = db.query(Db.TABLE_Marker, null, null, null, null, null, null);
		final List<Marker> res = new ArrayList<>();

		try {
			while (c.moveToNext()) {
				res.add(markerFromCursor(c));
			}
		} finally {
			c.close();
		}

		return res;
	}

	private SQLiteStatement stmt_countMarkersForBookChapter = null;

	public int countMarkersForBookChapter(int ari_bookchapter) {
		final int ariMin = ari_bookchapter & 0x00ffff00;
		final int ariMax = ari_bookchapter | 0x000000ff;

		if (stmt_countMarkersForBookChapter == null) {
			stmt_countMarkersForBookChapter = helper.getReadableDatabase().compileStatement("select count(*) from " + Db.TABLE_Marker + " where " + Db.Marker.ari + ">=? and " + Db.Marker.ari + "<?");
		}

		stmt_countMarkersForBookChapter.bindLong(1, ariMin);
		stmt_countMarkersForBookChapter.bindLong(2, ariMax);

		return (int) stmt_countMarkersForBookChapter.simpleQueryForLong();
	}


	/**
	 * Put attributes (bookmark count, note count, and highlight color) for each verse.
	 */
	public void putAttributes(final int ari_bookchapter, final int[] bookmarkCountMap, final int[] noteCountMap, final Highlights.Info[] highlightColorMap) {
		final int ariMin = ari_bookchapter & 0x00ffff00;
		final int ariMax = ari_bookchapter | 0x000000ff;

		final String[] params = {
			String.valueOf(ariMin),
			String.valueOf(ariMax),
		};

		// order by modifyTime, so in case a verse has more than one highlight, the latest one is shown
		final Cursor cursor = helper.getReadableDatabase().rawQuery("select * from " + Db.TABLE_Marker + " where " + Db.Marker.ari + ">=? and " + Db.Marker.ari + "<? order by " + Db.Marker.modifyTime, params);
		try {
			final int col_kind = cursor.getColumnIndexOrThrow(Db.Marker.kind);
			final int col_ari = cursor.getColumnIndexOrThrow(Db.Marker.ari);
			final int col_caption = cursor.getColumnIndexOrThrow(Db.Marker.caption);
			final int col_verseCount = cursor.getColumnIndexOrThrow(Db.Marker.verseCount);

			while (cursor.moveToNext()) {
				final int ari = cursor.getInt(col_ari);
				final int kind = cursor.getInt(col_kind);

				int mapOffset = Ari.toVerse(ari) - 1;
				if (mapOffset >= bookmarkCountMap.length) {
					Log.e(TAG, "mapOffset too many " + mapOffset + " happens on ari 0x" + Integer.toHexString(ari));
					continue;
				}

				if (kind == Marker.Kind.bookmark.code) {
					bookmarkCountMap[mapOffset] += 1;
				} else if (kind == Marker.Kind.note.code) {
					noteCountMap[mapOffset] += 1;
				} else if (kind == Marker.Kind.highlight.code) {
					// traverse as far as verseCount
					final int verseCount = cursor.getInt(col_verseCount);

					for (int i = 0; i < verseCount; i++) {
						int mapOffset2 = mapOffset + i;
						if (mapOffset2 >= highlightColorMap.length) break; // do not go past number of verses in this chapter

						final String caption = cursor.getString(col_caption);
						final Highlights.Info info = Highlights.decode(caption);

						highlightColorMap[mapOffset2] = info;
					}
				}
			}
		} finally {
			cursor.close();
		}
	}

	/**
	 * @param colorRgb may NOT be -1. Use {@link #updateOrInsertHighlights(int, IntArrayList, int)} to delete highlight.
	 */
	public void updateOrInsertPartialHighlight(final int ari, final int colorRgb, final CharSequence verseText, final int startOffset, final int endOffset) {
		final SQLiteDatabase db = helper.getWritableDatabase();

		db.beginTransactionNonExclusive();
		try {
			// order by modifyTime desc so we modify the latest one and remove earlier ones if they exist.
			final Cursor c = db.query(Db.TABLE_Marker, null, Db.Marker.ari + "=? and " + Db.Marker.kind + "=?", ToStringArray(ari, Marker.Kind.highlight.code), null, null, Db.Marker.modifyTime + " desc");
			try {
				final int hashCode = Highlights.hashCode(verseText.toString());
				final Date now = new Date();

				if (c.moveToNext()) { // check if marker exists
					{ // modify the latest one
						final Marker marker = markerFromCursor(c);
						marker.modifyTime = now;
						marker.caption = Highlights.encode(colorRgb, hashCode, startOffset, endOffset);
						db.update(Db.TABLE_Marker, markerToContentValues(marker), "_id=?", ToStringArray(marker._id));
					}

					// remove earlier ones if they exist (caused by sync)
					while (c.moveToNext()) {
						final long _id = c.getLong(c.getColumnIndexOrThrow("_id"));
						db.delete(Db.TABLE_Marker, "_id=?", ToStringArray(_id));
					}
				} else { // insert
					final Marker marker = Marker.createNewMarker(ari, Marker.Kind.highlight, Highlights.encode(colorRgb, hashCode, startOffset, endOffset), 1, now, now);
					db.insert(Db.TABLE_Marker, null, markerToContentValues(marker));
				}
			} finally {
				c.close();
			}
			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}

		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
	}

	public void updateOrInsertHighlights(int ari_bookchapter, IntArrayList selectedVerses_1, int colorRgb) {
		final SQLiteDatabase db = helper.getWritableDatabase();

		db.beginTransactionNonExclusive();
		try {
			final String[] params = ToStringArray(null /* for the ari */, Marker.Kind.highlight.code);

			// every requested verses
			for (int i = 0; i < selectedVerses_1.size(); i++) {
				final int ari = Ari.encodeWithBc(ari_bookchapter, selectedVerses_1.get(i));
				params[0] = String.valueOf(ari);

				// order by modifyTime desc so we modify the latest one and remove earlier ones if they exist.
				final Cursor c = db.query(Db.TABLE_Marker, null, Db.Marker.ari + "=? and " + Db.Marker.kind + "=?", params, null, null, Db.Marker.modifyTime + " desc");
				try {
					if (c.moveToNext()) { // check if marker exists
						{ // modify the latest one
							final Marker marker = markerFromCursor(c);
							marker.modifyTime = new Date();
							if (colorRgb != -1) {
								marker.caption = Highlights.encode(colorRgb);
								db.update(Db.TABLE_Marker, markerToContentValues(marker), "_id=?", ToStringArray(marker._id));
							} else {
								// delete entry
								db.delete(Db.TABLE_Marker, "_id=?", ToStringArray(marker._id));
							}
						}

						// remove earlier ones if they exist (caused by sync)
						while (c.moveToNext()) {
							final long _id = c.getLong(c.getColumnIndexOrThrow("_id"));
							db.delete(Db.TABLE_Marker, "_id=?", ToStringArray(_id));
						}
					} else {
						if (colorRgb == -1) {
							// no need to do, from no color to no color
						} else {
							final Date now = new Date();
							final Marker marker = Marker.createNewMarker(ari, Marker.Kind.highlight, Highlights.encode(colorRgb), 1, now, now);
							db.insert(Db.TABLE_Marker, null, markerToContentValues(marker));
						}
					}
				} finally {
					c.close();
				}
			}
			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}

		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
	}

	/**
	 * Get the highlight color rgb of several verses.
	 * @return the color rgb or -1 if there are multiple colors.
	 */
	public int getHighlightColorRgb(int ari_bookchapter, IntArrayList selectedVerses_1) {
		int ariMin = ari_bookchapter & 0xffffff00;
		int ariMax = ari_bookchapter | 0x000000ff;
		int[] colors = new int[256];
		int res = -2;

		for (int i = 0; i < colors.length; i++) colors[i] = -1;

		// check if exists
		final Cursor c = helper.getReadableDatabase().query(
			Db.TABLE_Marker, null, Db.Marker.ari + ">? and " + Db.Marker.ari + "<=? and " + Db.Marker.kind + "=?",
			new String[]{String.valueOf(ariMin), String.valueOf(ariMax), String.valueOf(Marker.Kind.highlight.code)},
			null, null, null
		);

		try {
			final int col_ari = c.getColumnIndexOrThrow(Db.Marker.ari);
			final int col_caption = c.getColumnIndexOrThrow(Db.Marker.caption);

			// put to array first
			while (c.moveToNext()) {
				int ari = c.getInt(col_ari);
				int index = ari & 0xff;
				final Highlights.Info info = Highlights.decode(c.getString(col_caption));
				colors[index] = info.colorRgb;
			}

			// determine default color. If all has color x, then it's x. If one of them is not x, then it's -1.
			for (int i = 0; i < selectedVerses_1.size(); i++) {
				int verse_1 = selectedVerses_1.get(i);
				int color = colors[verse_1];
				if (res == -2) {
					res = color;
				} else if (color != res) {
					return -1;
				}
			}

			if (res == -2) return -1;
			return res;
		} finally {
			c.close();
		}
	}

	/**
	 * Get the highlight info for a single verse
	 */
	public Highlights.Info getHighlightColorRgb(final int ari) {
		try (Cursor c = helper.getReadableDatabase().query(
			Db.TABLE_Marker, null, Db.Marker.ari + "=? and " + Db.Marker.kind + "=?",
			ToStringArray(ari, Marker.Kind.highlight.code),
			null,
			null,
			Db.Marker.modifyTime + " desc"
		)) {
			final int col_caption = c.getColumnIndexOrThrow(Db.Marker.caption);

			// put to array first
			if (c.moveToNext()) {
				return Highlights.decode(c.getString(col_caption));
			} else {
				return null;
			}
		}
	}

	public void storeArticleToDevotions(DevotionArticle article) {
		final SQLiteDatabase db = helper.getWritableDatabase();

		final ContentValues values = new ContentValues();
		values.put(Table.Devotion.name.name(), article.getKind().name);
		values.put(Table.Devotion.date.name(), article.getDate());
		values.put(Table.Devotion.readyToUse.name(), article.getReadyToUse() ? 1 : 0);

		if (article.getReadyToUse()) {
			values.put(Table.Devotion.body.name(), article.getBody());
		} else {
			values.putNull(Table.Devotion.body.name());
		}

		values.put(Table.Devotion.touchTime.name(), Sqlitil.nowDateTime());
		values.put(Table.Devotion.dataFormatVersion.name(), 1);

		db.beginTransactionNonExclusive();
		try {
			// first delete the existing
			db.delete(Table.Devotion.tableName(), Table.Devotion.name + "=? and " + Table.Devotion.date + "=?", new String[]{article.getKind().name, article.getDate()});
			db.insert(Table.Devotion.tableName(), null, values);

			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}
	}

	public int deleteDevotionsWithTouchTimeBefore(Date date) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		return db.delete(Table.Devotion.tableName(), Table.Devotion.touchTime + "<?", ToStringArray(Sqlitil.toInt(date)));
	}

	/**
	 * Try to get article from local db. Non ready-to-use article will be returned too.
	 */
	public DevotionArticle tryGetDevotion(String name, String date) {
		try (Cursor c = helper.getReadableDatabase().query(Table.Devotion.tableName(), null, Table.Devotion.name + "=? and " + Table.Devotion.date + "=? and " + Table.Devotion.dataFormatVersion + "=?", ToStringArray(name, date, 1), null, null, null)) {
			final int col_body = c.getColumnIndexOrThrow(Table.Devotion.body.name());
			final int col_readyToUse = c.getColumnIndexOrThrow(Table.Devotion.readyToUse.name());

			if (!c.moveToNext()) {
				return null;
			}

			final DevotionActivity.DevotionKind kind = DevotionActivity.DevotionKind.getByName(name);
			switch (kind) {
				case RH: {
					return new ArticleRenunganHarian(date, c.getString(col_body), c.getInt(col_readyToUse) > 0);
				}
				case SH: {
					return new ArticleSantapanHarian(date, c.getString(col_body), c.getInt(col_readyToUse) > 0);
				}
				case ME_EN: {
					return new ArticleMorningEveningEnglish(date, c.getString(col_body), true);
				}
				case MEID_A: {
					return new ArticleMeidA(date, c.getString(col_body), c.getInt(col_readyToUse) > 0);
				}
				case ROC: {
					return new ArticleRoc(date, c.getString(col_body), c.getInt(col_readyToUse) > 0);
				}
				case REFHEART: {
					return new ArticleRefheart(date, c.getString(col_body), c.getInt(col_readyToUse) > 0);
				}
			}
		}

		throw new RuntimeException("Should not be reachable");
	}

	public List<MVersionDb> listAllVersions() {
		List<MVersionDb> res = new ArrayList<>();
		Cursor cursor = helper.getReadableDatabase().query(Db.TABLE_Version, null, null, null, null, null, Db.Version.ordering + " asc");
		try {
			int col_locale = cursor.getColumnIndexOrThrow(Db.Version.locale);
			int col_shortName = cursor.getColumnIndexOrThrow(Db.Version.shortName);
			int col_longName = cursor.getColumnIndexOrThrow(Db.Version.longName);
			int col_description = cursor.getColumnIndexOrThrow(Db.Version.description);
			int col_filename = cursor.getColumnIndexOrThrow(Db.Version.filename);
			int col_preset_name = cursor.getColumnIndexOrThrow(Db.Version.preset_name);
			int col_modifyTime = cursor.getColumnIndexOrThrow(Db.Version.modifyTime);
			int col_active = cursor.getColumnIndexOrThrow(Db.Version.active);
			int col_ordering = cursor.getColumnIndexOrThrow(Db.Version.ordering);

			while (cursor.moveToNext()) {
				final MVersionDb mv = new MVersionDb();
				mv.locale = cursor.getString(col_locale);
				mv.shortName = cursor.getString(col_shortName);
				mv.longName = cursor.getString(col_longName);
				mv.description = cursor.getString(col_description);
				mv.filename = cursor.getString(col_filename);
				mv.preset_name = cursor.getString(col_preset_name);
				mv.modifyTime = cursor.getInt(col_modifyTime);
				mv.cache_active = cursor.getInt(col_active) != 0;
				mv.ordering = cursor.getInt(col_ordering);
				res.add(mv);
			}
		} finally {
			cursor.close();
		}
		return res;
	}

	public void setVersionActive(MVersionDb mv, boolean active) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		final ContentValues cv = new ContentValues();
		cv.put(Db.Version.active, active? 1: 0);

		if (mv.preset_name != null) {
			db.update(Db.TABLE_Version, cv, Db.Version.preset_name + "=?", new String[] {mv.preset_name});
		} else {
			db.update(Db.TABLE_Version, cv, Db.Version.filename + "=?", new String[] {mv.filename});
		}
	}

	public int getVersionMaxOrdering() {
		final SQLiteDatabase db = helper.getReadableDatabase();
		return (int) DatabaseUtils.longForQuery(db, "select max(" + Db.Version.ordering + ") from " + Db.TABLE_Version, null);
	}

	public void insertVersionWithActive(MVersionDb mv, boolean active) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		final ContentValues cv = new ContentValues();
		cv.put(Db.Version.locale, mv.locale);
		cv.put(Db.Version.shortName, mv.shortName);
		cv.put(Db.Version.longName, mv.longName);
		cv.put(Db.Version.description, mv.description);
		cv.put(Db.Version.filename, mv.filename);
		cv.put(Db.Version.preset_name, mv.preset_name);
		cv.put(Db.Version.modifyTime, mv.modifyTime);
		cv.put(Db.Version.active, active); // special
		cv.put(Db.Version.ordering, mv.ordering);

		db.beginTransactionNonExclusive();
		try { // prevent insert for the same filename (absolute path), update instead
			final long count = DatabaseUtils.queryNumEntries(db, Db.TABLE_Version, Db.Version.filename + "=?", new String[]{mv.filename});
			if (count == 0) {
				db.insert(Db.TABLE_Version, null, cv);
			} else {
				db.update(Db.TABLE_Version, cv, Db.Version.filename + "=?", new String[]{mv.filename});
			}

			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}
	}

	public boolean hasVersionWithFilename(String filename) {
		final SQLiteDatabase db = helper.getReadableDatabase();
		return DatabaseUtils.longForQuery(db, "select count(*) from " + Db.TABLE_Version + " where " + Db.Version.filename + "=?", new String[] {filename}) > 0;
	}

	public void deleteVersion(MVersionDb mv) {
		final SQLiteDatabase db = helper.getWritableDatabase();

		// delete preset by preset_name
		if (mv.preset_name != null) {
			final int deleted = db.delete(Db.TABLE_Version, Db.Version.preset_name + "=?", new String[]{mv.preset_name});
			if (deleted > 0) {
				return; // finished! if not, we fallback to filename
			}
		}

		db.delete(Db.TABLE_Version, Db.Version.filename + "=?", new String[]{mv.filename});
	}

	public List<Label> listAllLabels() {
		List<Label> res = new ArrayList<>();
		Cursor cursor = helper.getReadableDatabase().query(Db.TABLE_Label, null, null, null, null, null, Db.Label.ordering + " asc");
		try {
			while (cursor.moveToNext()) {
				res.add(labelFromCursor(cursor));
			}
		} finally {
			cursor.close();
		}
		return res;
	}

	public List<Marker_Label> listAllMarker_Labels() {
		final List<Marker_Label> res = new ArrayList<>();
		final Cursor cursor = helper.getReadableDatabase().query(Db.TABLE_Marker_Label, null, null, null, null, null, null);
		try {
			while (cursor.moveToNext()) {
				res.add(marker_LabelFromCursor(cursor));
			}
		} finally {
			cursor.close();
		}
		return res;
	}

	public List<Marker_Label> listMarker_LabelsByMarker(final Marker marker) {
		final List<Marker_Label> res = new ArrayList<>();
		final Cursor cursor = helper.getReadableDatabase().query(Db.TABLE_Marker_Label, null, Db.Marker_Label.marker_gid + "=?", ToStringArray(marker.gid), null, null, null);
		try {
			while (cursor.moveToNext()) {
				res.add(marker_LabelFromCursor(cursor));
			}
		} finally {
			cursor.close();
		}
		return res;
	}

	public List<Label> listLabelsByMarker(final Marker marker) {
		final List<Label> res = new ArrayList<>();
		final Cursor cursor = helper.getReadableDatabase().rawQuery("select " + Db.TABLE_Label + ".* from " + Db.TABLE_Label + ", " + Db.TABLE_Marker_Label + " where " + Db.TABLE_Marker_Label + "." + Db.Marker_Label.label_gid + " = " + Db.TABLE_Label + "." + Db.Label.gid + " and " + Db.TABLE_Marker_Label + "." + Db.Marker_Label.marker_gid + "=? order by " + Db.TABLE_Label + "." + Db.Label.ordering + " asc", Array(marker.gid));
		try {
			while (cursor.moveToNext()) {
				res.add(labelFromCursor(cursor));
			}
		} finally {
			cursor.close();
		}
		return res;
	}

	public static Label labelFromCursor(Cursor c) {
		final Label res = Label.createEmptyLabel();

		res._id = c.getLong(c.getColumnIndexOrThrow("_id"));
		res.gid = c.getString(c.getColumnIndexOrThrow(Db.Label.gid));
		res.title = c.getString(c.getColumnIndexOrThrow(Db.Label.title));
		res.ordering = c.getInt(c.getColumnIndexOrThrow(Db.Label.ordering));
		res.backgroundColor = c.getString(c.getColumnIndexOrThrow(Db.Label.backgroundColor));

		return res;
	}

	/**
	 * _id is not stored
	 */
	private static ContentValues labelToContentValues(Label label) {
		final ContentValues res = new ContentValues();

		res.put(Db.Label.gid, label.gid);
		res.put(Db.Label.title, label.title);
		res.put(Db.Label.ordering, label.ordering);
		res.put(Db.Label.backgroundColor, label.backgroundColor);

		return res;
	}

	/**
	 * _id is not stored
	 */
	@NonNull private static ContentValues marker_labelToContentValues(@NonNull Marker_Label marker_label) {
		final ContentValues res = new ContentValues();

		res.put(Db.Marker_Label.gid, marker_label.gid);
		res.put(Db.Marker_Label.marker_gid, marker_label.marker_gid);
		res.put(Db.Marker_Label.label_gid, marker_label.label_gid);

		return res;
	}

	public int getLabelMaxOrdering() {
		SQLiteDatabase db = helper.getReadableDatabase();
		SQLiteStatement stmt = db.compileStatement("select max(" + Db.Label.ordering + ") from " + Db.TABLE_Label);
		try {
			return (int) stmt.simpleQueryForLong();
		} finally {
			stmt.close();
		}
	}

	public Label insertLabel(String title, String bgColor) {
		final Label res = Label.createNewLabel(title, getLabelMaxOrdering() + 1, bgColor);
		final SQLiteDatabase db = helper.getWritableDatabase();

		res._id = db.insert(Db.TABLE_Label, null, labelToContentValues(res));
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
		return res;
	}

	public void updateLabels(final Marker marker, final Set<Label> newLabels) {
		final SQLiteDatabase db = helper.getWritableDatabase();

		db.beginTransactionNonExclusive();
		try {
			final List<Marker_Label> oldMls = listMarker_LabelsByMarker(marker);

			// helper list
			final List<String> oldMlLabelGids = new ArrayList<>();
			for (final Marker_Label oldMl : oldMls) {
				oldMlLabelGids.add(oldMl.label_gid);
			}


			// calculate labels to be added
			final List<Label> addLabels = new ArrayList<>();

			for (final Label newLabel : newLabels) {
				if (!oldMlLabelGids.contains(newLabel.gid)) {
					addLabels.add(newLabel);
				}
			}

			// calculate marker_labels to be removed
			final List<Marker_Label> removeMls = new ArrayList<>();
			{
				// helper list
				final List<String> newLabelGids = new ArrayList<>();
				for (final Label newLabel : newLabels) {
					newLabelGids.add(newLabel.gid);
				}

				for (int i = 0; i < oldMls.size(); i++) {
					final Marker_Label oldMl = oldMls.get(i);

					// look for duplicate labels
					if (oldMlLabelGids.subList(i + 1, oldMlLabelGids.size()).contains(oldMl.label_gid)) {
						removeMls.add(oldMl);
						continue;
					}

					// if the old one is not in the new ones
					if (!newLabelGids.contains(oldMl.label_gid)) {
						removeMls.add(oldMl);
					}
				}
			}

			// remove
			for (final Marker_Label removeMl : removeMls) {
				db.delete(Db.TABLE_Marker_Label, "_id=?", ToStringArray(removeMl._id));
			}

			// add
			for (final Label addLabel : addLabels) {
				final Marker_Label marker_label = Marker_Label.createNewMarker_Label(marker.gid, addLabel.gid);
				db.insert(Db.TABLE_Marker_Label, null, marker_labelToContentValues(marker_label));
			}

			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
	}

    public Label getLabelById(long _id) {
        SQLiteDatabase db = helper.getReadableDatabase();
		Cursor cursor = db.query(Db.TABLE_Label, null, "_id=?", new String[]{String.valueOf(_id)}, null, null, null);
		try {
            if (cursor.moveToNext()) {
                return labelFromCursor(cursor);
            } else {
                return null;
            }
        } finally {
            cursor.close();
        }
    }

    @Nullable public Label getLabelByGid(@NonNull final String gid) {
		final Cursor cursor = helper.getReadableDatabase().query(Db.TABLE_Label, null, Db.Label.gid + "=?", Array(gid), null, null, null);

		try {
			if (!cursor.moveToNext()) return null;
			return labelFromCursor(cursor);
		} finally {
			cursor.close();
		}
	}

    @Nullable public Marker_Label getMarker_LabelByGid(@NonNull final String gid) {
		final Cursor cursor = helper.getReadableDatabase().query(Db.TABLE_Marker_Label, null, Db.Marker_Label.gid + "=?", Array(gid), null, null, null);

		try {
			if (!cursor.moveToNext()) return null;
			return marker_LabelFromCursor(cursor);
		} finally {
			cursor.close();
		}
	}

	/** This is so special: delete label and the associated marker_labels */
	public void deleteLabelAndMarker_LabelsByLabelId(long _id) {
		final Label label = getLabelById(_id);
		final SQLiteDatabase db = helper.getWritableDatabase();
		db.beginTransactionNonExclusive();
		try {
			db.delete(Db.TABLE_Marker_Label, Db.Marker_Label.label_gid + "=?", new String[]{label.gid});
			db.delete(Db.TABLE_Label, "_id=?", new String[]{String.valueOf(_id)});
			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
	}

	/**
	 * Insert a new label or update an existing label.
	 * @param label if the _id is 0, this label will be inserted. Otherwise, updated.
	 */
	public void insertOrUpdateLabel(@NonNull final Label label) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		if (label._id != 0) {
			db.update(Db.TABLE_Label, labelToContentValues(label), "_id=?", Array(String.valueOf(label._id)));
		} else {
			label._id = db.insert(Db.TABLE_Label, null, labelToContentValues(label));
		}
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
	}

	/**
	 * Insert a new marker-label association or update an existing one.
	 * @param marker_label if the _id is 0, this label will be inserted. Otherwise, updated.
	 */
	public void insertOrUpdateMarker_Label(@NonNull final Marker_Label marker_label) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		if (marker_label._id != 0) {
			db.update(Db.TABLE_Marker_Label, marker_labelToContentValues(marker_label), "_id=?", ToStringArray(marker_label._id));
		} else {
			marker_label._id = db.insert(Db.TABLE_Marker_Label, null, marker_labelToContentValues(marker_label));
		}
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
	}

	/** Used in migration from v3 */
	public static long insertMarker_LabelIfNotExists(final SQLiteDatabase db, final Marker_Label marker_label) {
		db.beginTransactionNonExclusive();
		try {
			final Cursor cursor = db.rawQuery("select _id from " + Db.TABLE_Marker_Label + " where " + Db.Marker_Label.marker_gid + "=? and " + Db.Marker_Label.label_gid + "=?", Array(marker_label.marker_gid, marker_label.label_gid));
			try {
				if (cursor.moveToNext()) {
					marker_label._id = cursor.getLong(0);
				} else {
					marker_label._id = db.insert(Db.TABLE_Marker_Label, null, marker_labelToContentValues(marker_label));
				}
			} finally {
				cursor.close();
			}
			db.setTransactionSuccessful();
		} finally {
            db.endTransaction();
		}

		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);

		return marker_label._id;
	}

	public int countMarkersWithLabel(Label label) {
		final SQLiteDatabase db = helper.getReadableDatabase();
		return (int) DatabaseUtils.longForQuery(db, "select count(*) from " + Db.TABLE_Marker_Label + " where " + Db.Marker_Label.label_gid + "=?", new String[]{label.gid});
	}

	public void sortLabelsAlphabetically() {
		final SQLiteDatabase db = helper.getWritableDatabase();
		db.beginTransactionNonExclusive();
		try {
			final List<Label> labels = listAllLabels();
			Collections.sort(labels, (lhs, rhs) -> {
				if (lhs.title == null || rhs.title == null) {
					return 0;
				}

				return lhs.title.compareToIgnoreCase(rhs.title);
			});

			for (int i = 0; i < labels.size(); i++) {
				final Label label = labels.get(i);
				label.ordering = i + 1;
				db.update(Db.TABLE_Label, labelToContentValues(label), "_id=?", ToStringArray(label._id));
			}

			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
	}

	public void reorderLabels(Label from, Label to) {
		// original order: A101 B[102] C103 D[104] E105

		// case: move up from=104 to=102:
		//   increase ordering for (to <= ordering < from)
		//   A101 B[103] C104 D[104] E105
		//   replace ordering of 'from' to 'to'
		//   A101 B[103] C104 D[102] E105

		// case: move down from=102 to=104:
		//   decrease ordering for (from < ordering <= to)
		//   A101 B[102] C102 D[103] E105
		//   replace ordering of 'from' to 'to'
		//   A101 B[104] C102 D[103] E105

		if (D.EBUG) {
			Log.d(TAG, "@@reorderLabels from _id=" + from._id + " ordering=" + from.ordering + " to _id=" + to._id + " ordering=" + to.ordering);
		}

		SQLiteDatabase db = helper.getWritableDatabase();
		db.beginTransactionNonExclusive();
		try {
			if (from.ordering > to.ordering) { // move up
				db.execSQL("update " + Db.TABLE_Label + " set " + Db.Label.ordering + "=(" + Db.Label.ordering + "+1) where ?<=" + Db.Label.ordering + " and " + Db.Label.ordering + "<?", new Object[] {to.ordering, from.ordering});
				db.execSQL("update " + Db.TABLE_Label + " set " + Db.Label.ordering + "=? where _id=?", new Object[] {to.ordering, from._id});
			} else if (from.ordering < to.ordering) { // move down
				db.execSQL("update " + Db.TABLE_Label + " set " + Db.Label.ordering + "=(" + Db.Label.ordering + "-1) where ?<" + Db.Label.ordering + " and " + Db.Label.ordering + "<=?", new Object[] {from.ordering, to.ordering});
				db.execSQL("update " + Db.TABLE_Label + " set " + Db.Label.ordering + "=? where _id=?", new Object[] {to.ordering, from._id});
			}

			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
	}

	public void reorderVersions(MVersion from, MVersion to) {
		// original order: A101 B[102] C103 D[104] E105

		// case: move up from=104 to=102:
		//   increase ordering for (to <= ordering < from)
		//   A101 B[103] C104 D[104] E105
		//   replace ordering of 'from' to 'to'
		//   A101 B[103] C104 D[102] E105

		// case: move down from=102 to=104:
		//   decrease ordering for (from < ordering <= to)
		//   A101 B[102] C102 D[103] E105
		//   replace ordering of 'from' to 'to'
		//   A101 B[104] C102 D[103] E105

		if (BuildConfig.DEBUG) {
			Log.d(TAG, "@@reorderVersions from id=" + from.getVersionId() + " ordering=" + from.ordering + " to id=" + to.getVersionId() + " ordering=" + to.ordering);
		}

		SQLiteDatabase db = helper.getWritableDatabase();
		db.beginTransactionNonExclusive();
		try {
			{
				final int internal_ordering = Preferences.getInt(Prefkey.internal_version_ordering, MVersionInternal.DEFAULT_ORDERING);
				if (from.ordering > to.ordering) { // move up
					db.execSQL("update " + Db.TABLE_Version + " set " + Db.Version.ordering + "=(" + Db.Version.ordering + "+1) where ?<=" + Db.Version.ordering + " and " + Db.Version.ordering + "<?", new Object[]{to.ordering, from.ordering});
					if (to.ordering <= internal_ordering && internal_ordering < from.ordering) {
						Preferences.setInt(Prefkey.internal_version_ordering, internal_ordering + 1);
					}
				} else if (from.ordering < to.ordering) { // move down
					db.execSQL("update " + Db.TABLE_Version + " set " + Db.Version.ordering + "=(" + Db.Version.ordering + "-1) where ?<" + Db.Version.ordering + " and " + Db.Version.ordering + "<=?", new Object[]{from.ordering, to.ordering});
					if (from.ordering < internal_ordering && internal_ordering <= to.ordering) {
						Preferences.setInt(Prefkey.internal_version_ordering, internal_ordering - 1);
					}
				}
			}

			// both move up and move down arrives at this final step
			if (from instanceof MVersionDb) {
				db.execSQL("update " + Db.TABLE_Version + " set " + Db.Version.ordering + "=? where " + Db.Version.filename + "=?", new Object[]{to.ordering, ((MVersionDb) from).filename});
			} else if (from instanceof MVersionInternal) {
				Preferences.setInt(Prefkey.internal_version_ordering, to.ordering);
			}

			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}
	}

	/**
	 * Lists all progress marks that are not empty.
	 * (Empty ones will have an ari of 0. They will be excluded.)
	 */
	public List<ProgressMark> listAllProgressMarks() {
		final List<ProgressMark> res = new ArrayList<>();
		final Cursor cursor = helper.getReadableDatabase().query(Db.TABLE_ProgressMark, null, Db.ProgressMark.ari + " != 0", null, null, null, null);
		try {
			while (cursor.moveToNext()) {
				res.add(progressMarkFromCursor(cursor));
			}
		} finally {
			cursor.close();
		}

		return res;
	}

	/**
	 * Count the number of progress marks that are not empty.
	 * (Empty ones will have an ari of 0. They will be excluded.)
	 */
	public int countAllProgressMarks() {
		return (int) DatabaseUtils.queryNumEntries(helper.getReadableDatabase(), Db.TABLE_ProgressMark, Db.ProgressMark.ari + " != 0");
	}

	@Nullable public ProgressMark getProgressMarkByPresetId(final int preset_id) {
		Cursor cursor = helper.getReadableDatabase().query(
			Db.TABLE_ProgressMark,
			null,
			Db.ProgressMark.preset_id + "=?",
			new String[]{String.valueOf(preset_id)},
			null, null, null
		);

		try {
			if (!cursor.moveToNext()) return null;

			return progressMarkFromCursor(cursor);
		} finally {
			cursor.close();
		}
	}

	/**
	 * Insert a new progress mark (if preset_id is not found), or update an existing progress mark.
	 */
	public void insertOrUpdateProgressMark(@NonNull final ProgressMark progressMark) {
		final SQLiteDatabase db = helper.getWritableDatabase();

		final ContentValues cv = new ContentValues();
		cv.put(Db.ProgressMarkHistory.progress_mark_preset_id, progressMark.preset_id);
		cv.put(Db.ProgressMarkHistory.progress_mark_caption, progressMark.caption);
		cv.put(Db.ProgressMarkHistory.ari, progressMark.ari);
		cv.put(Db.ProgressMarkHistory.createTime, Sqlitil.toInt(progressMark.modifyTime));

		db.beginTransactionNonExclusive();
		try {
			// the progress mark history first
			db.insert(Db.TABLE_ProgressMarkHistory, null, cv);

			final long count = DatabaseUtils.queryNumEntries(db, Db.TABLE_ProgressMark, Db.ProgressMark.preset_id + "=?", ToStringArray(progressMark.preset_id));
			if (count > 0) {
				db.update(Db.TABLE_ProgressMark, progressMarkToContentValues(progressMark), Db.ProgressMark.preset_id + "=?", ToStringArray(progressMark.preset_id));
			} else {
				db.insert(Db.TABLE_ProgressMark, null, progressMarkToContentValues(progressMark));
			}

			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}

		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_PINS);
	}

	public List<ProgressMarkHistory> listProgressMarkHistoryByPresetId(final int preset_id) {
		final Cursor c = helper.getReadableDatabase().rawQuery("select * from " + Db.TABLE_ProgressMarkHistory + " where " + Db.ProgressMarkHistory.progress_mark_preset_id + "=? order by " + Db.ProgressMarkHistory.createTime + " asc", new String[]{String.valueOf(preset_id)});
		try {
			final List<ProgressMarkHistory> res = new ArrayList<>();
			while (c.moveToNext()) {
				res.add(progressMarkHistoryFromCursor(c));
			}
			return res;
		} finally {
			c.close();
		}
	}

	public static ProgressMark progressMarkFromCursor(Cursor c) {
		ProgressMark res = new ProgressMark();
		res._id = c.getLong(c.getColumnIndexOrThrow(BaseColumns._ID));
		res.preset_id = c.getInt(c.getColumnIndexOrThrow(Db.ProgressMark.preset_id));
		res.caption = c.getString(c.getColumnIndexOrThrow(Db.ProgressMark.caption));
		res.ari = c.getInt(c.getColumnIndexOrThrow(Db.ProgressMark.ari));
		res.modifyTime = Sqlitil.toDate(c.getInt(c.getColumnIndexOrThrow(Db.ProgressMark.modifyTime)));

		return res;
	}

	public static ContentValues progressMarkToContentValues(ProgressMark progressMark) {
		ContentValues cv = new ContentValues();
		cv.put(Db.ProgressMark.preset_id, progressMark.preset_id);
		cv.put(Db.ProgressMark.caption, progressMark.caption);
		cv.put(Db.ProgressMark.ari, progressMark.ari);
		cv.put(Db.ProgressMark.modifyTime, Sqlitil.toInt(progressMark.modifyTime));
		return cv;
	}

	public static ProgressMarkHistory progressMarkHistoryFromCursor(Cursor c) {
		final ProgressMarkHistory res = new ProgressMarkHistory();
		res._id = c.getLong(c.getColumnIndexOrThrow(BaseColumns._ID));
		res.progress_mark_preset_id = c.getInt(c.getColumnIndexOrThrow(Db.ProgressMarkHistory.progress_mark_preset_id));
		res.progress_mark_caption = c.getString(c.getColumnIndexOrThrow(Db.ProgressMarkHistory.progress_mark_caption));
		res.ari = c.getInt(c.getColumnIndexOrThrow(Db.ProgressMarkHistory.ari));
		res.createTime = Sqlitil.toDate(c.getInt(c.getColumnIndexOrThrow(Db.ProgressMarkHistory.createTime)));

		return res;
	}

	public long insertReadingPlan(final ReadingPlan.ReadingPlanInfo info, byte[] data) {
		final ContentValues cv = new ContentValues();
		cv.put(Db.ReadingPlan.version, info.version);
		cv.put(Db.ReadingPlan.name, info.name);
		cv.put(Db.ReadingPlan.title, info.title);
		cv.put(Db.ReadingPlan.description, info.description);
		cv.put(Db.ReadingPlan.duration, info.duration);
		cv.put(Db.ReadingPlan.startTime, info.startTime);
		cv.put(Db.ReadingPlan.data, data);
		final long res = helper.getWritableDatabase().insert(Db.TABLE_ReadingPlan, null, cv);

		// this adds the 'startTime' attribute to the sync entity (when any of the rp progress has been checked)
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_RP);

		return res;
	}

	public void insertOrUpdateReadingPlanProgress(final String gid, final int readingCode, final long checkTime) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		db.beginTransactionNonExclusive();
		try {
			db.delete(Db.TABLE_ReadingPlanProgress, Db.ReadingPlanProgress.reading_plan_progress_gid + "=? and " + Db.ReadingPlanProgress.reading_code + "=?", ToStringArray(gid, readingCode));

			final ContentValues cv = new ContentValues();
			cv.put(Db.ReadingPlanProgress.reading_plan_progress_gid, gid);
			cv.put(Db.ReadingPlanProgress.reading_code, readingCode);
			cv.put(Db.ReadingPlanProgress.checkTime, checkTime);
			db.insert(Db.TABLE_ReadingPlanProgress, null, cv);

			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}

		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_RP);
	}

	public void insertOrUpdateMultipleReadingPlanProgresses(final String gid, final IntArrayList readingCodes, final long checkTime) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		db.beginTransactionNonExclusive();
		try {
			final ContentValues cv = new ContentValues();
			cv.put(Db.ReadingPlanProgress.reading_plan_progress_gid, gid);
			cv.put(Db.ReadingPlanProgress.checkTime, checkTime);

			for (int i = 0, len = readingCodes.size(); i < len; i++) {
				final int readingCode = readingCodes.get(i);

				db.delete(Db.TABLE_ReadingPlanProgress, Db.ReadingPlanProgress.reading_plan_progress_gid + "=? and " + Db.ReadingPlanProgress.reading_code + "=?", ToStringArray(gid, readingCode));

				// specific update
				cv.put(Db.ReadingPlanProgress.reading_code, readingCode);

				db.insert(Db.TABLE_ReadingPlanProgress, null, cv);
			}

			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}

		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_RP);
	}

	public void deleteReadingPlanProgress(final String gid, final int readingCode) {
		helper.getWritableDatabase().delete(Db.TABLE_ReadingPlanProgress, Db.ReadingPlanProgress.reading_plan_progress_gid + "=? and " + Db.ReadingPlanProgress.reading_code + "=?", ToStringArray(gid, readingCode));

		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_RP);
	}

	public void deleteAllReadingPlanProgressForGid(final String gid) {
		helper.getWritableDatabase().delete(Db.TABLE_ReadingPlanProgress, Db.ReadingPlanProgress.reading_plan_progress_gid + "=?", Array(gid));

		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_RP);
	}

	/**
	 * Get the list of reading plan gid with their done reading codes.
	 * The only source of data is from ReadingPlanProgress table, but since reading plans with no done is not listed in ReadingPlanProgress,
	 * please take care of it.
	 */
	public Map<String /* gid */, TIntSet /* done reading codes */> getReadingPlanProgressSummaryForSync() {
		final SQLiteDatabase db = helper.getReadableDatabase();
		final Map<String, TIntSet> res = new HashMap<>();
		try (Cursor c = db.query(Db.TABLE_ReadingPlanProgress, Array(Db.ReadingPlanProgress.reading_plan_progress_gid, Db.ReadingPlanProgress.reading_code), null, null, null, null, null)) {
			while (c.moveToNext()) {
				final String gid = c.getString(0);
				final int readingCode = c.getInt(1);

				TIntSet set = res.get(gid);
				if (set == null) {
					set = new TIntHashSet();
					res.put(gid, set);
				}

				set.add(readingCode);
			}
		}

		return res;
	}

	public static final String TBS_READINGPLAN_DATA_HEX = "528A613400E0EA019104046E616D65576D636865796E65057469746C65701B4D27436865796E65204269626C652052" +
		"656164696E6720506C616E0B6465736372697074696F6E705A4461696C79204F6C642054657374616D656E742C204E65772054657374616D656E742C20616E64205073616C6D7320" +
		"6F7220476F7370656C7320287777772E6573762E6F72672F6269626C6572656164696E67706C616E73292E086475726174696F6E20016D0800000100" +
		"000001000027010000270100000E0100000E0100002B0100002B01000800000200000002000027020000270200000E0200000E0200002B0200002B02" +
		"000800000300000003000027030000270300000E0300000E0300002B0300002B03000800000400000004000027040000270400000E0400000E040000" +
		"2B0400002B04000800000500000005000027050000270500000E0500000E0500002B0500002B05000800000600000006000027060000270600000E06" +
		"00000E0600002B0600002B06000800000700000007000027070000270700000E0700000E0700002B0700002B07000800000800000008000027080000" +
		"270800000E0800000E0800002B0800002B0800080000090000000A000027090000270900000E0900000E0900002B0900002B09000800000B0000000B" +
		"0000270A0000270A00000E0A00000E0A00002B0A00002B0A000800000C0000000C0000270B0000270B00000F0100000F0100002B0B00002B0B000800" +
		"000D0000000D0000270C0000270C00000F0200000F0200002B0C00002B0C000800000E0000000E0000270D0000270D00000F0300000F0300002B0D00" +
		"002B0D000800000F0000000F0000270E0000270E00000F0400000F0400002B0E00002B0E0008000010000000100000270F0000270F00000F0500000F" +
		"0500002B0F00002B0F000800001100000011000027100000271000000F0600000F0600002B1000002B10000800001200000012000027110000271100" +
		"000F0700000F0700002B1100002B11000800001300000013000027120000271200000F0800000F0800002B1200002B12000800001400000014000027" +
		"130000271300000F0900000F0900002B1300002B13000800001500000015000027140000271400000F0A00000F0A00002B1400002B14000800001600" +
		"000016000027150000271500000F0B00000F0B00002B1500002B15000800001700000017000027160000271600000F0C00000F0C00002B1600002B16" +
		"000800001800000018000027170000271700000F0D00000F0D00002B1700002B17000800001900000019000027180000271800001001000010010000" +
		"2B1800002B18000800001A0000001A0000271900002719000010020000100200002B1900002B19000800001B0000001B0000271A0000271A00001003" +
		"0000100300002B1A00002B1A000800001C0000001C0000271B0000271B000010040000100400002B1B00002B1B000800001D0000001D0000271C0000" +
		"271C000010050000100500002B1C00002B1C000800001E0000001E0000280100002801000010060000100600002C0100002C01000800001F0000001F" +
		"0000280200002802000010070000100700002C0200002C020008000020000000200000280300002803000010080000100800002C0300002C03000800" +
		"0021000000210000280400002804000010090000100A00002C0400002C040008000022000000220000280500002805000011010000110100002C0500" +
		"002C050008000023000000240000280600002806000011020000110200002C0600002C06000800002500000025000028070000280700001103000011" +
		"0300002C0700002C070008000026000000260000280800002808000011040000110400002C0800002C08000800002700000027000028090000280900" +
		"0011050000110500002C0900002C090008000028000000280000280A0000280A000011060000110600002C0A00002C0A000800002900000029000028" +
		"0B0000280B000011070000110700002C0B00002C0B000800002A0000002A0000280C0000280C000011080000110800002C0C00002C0C000800002B00" +
		"00002B0000280D0000280D000011090000110900002C0D00002C0D000800002C0000002C0000280E0000280E0000110A0000110A00002C0E00002C0E" +
		"000800002D0000002D0000280F0000280F0000110B0000110B00002C0F00002C0F000800002E0000002E00002810000028100000110C0000110C0000" +
		"2C1000002C10000800002F0000002F00002901010029012600110D0000110D00002D0100002D0100080000300000003000002901270029015000110E" +
		"0000110E00002D0200002D0200080000310000003100002902000029020000110F0000110F00002D0300002D03000800003200000032000029030000" +
		"2903000011100000111100002D0400002D040008000101000001010000290400002904000011120000111200002D0500002D05000800010200000102" +
		"0000290500002905000011130000111300002D0600002D060008000103000001030000290600002906000011140000111400002D0700002D07000800" +
		"0104000001040000290700002907000011150000111500002D0800002D080008000105000001050000290800002908000011160000111600002D0900" +
		"002D090008000106000001060000290900002909000011170000111700002D0A00002D0A0008000107000001070000290A0000290A00001118000011" +
		"1800002D0B00002D0B0008000108000001080000290B0000290B000011190000111A00002D0C00002D0C0008000109000001090000290C0000290C00" +
		"00111B0000111B00002D0D00002D0D000800010A0000010A0000290D0000290D0000111C0000111C00002D0E00002D0E000800010B0000010B000029" +
		"0E0000290E0000111D0000111D00002D0F00002D0F000800010C0000010C0000290F0000290F0000111E0000111E00002D1000002D10000800010D00" +
		"00010D00002910000029100000111F0000111F00002E0100002E01000800010E0000010E0000291100002911000011200000112000002E0200002E02" +
		"000800010F0000010F0000291200002912000011210000112100002E0300002E03000800011000000110000029130000291300001122000011220000" +
		"2E0400002E040008000111000001110000291400002914000011230000112300002E0500002E05000800011200000112000029150000291500001124" +
		"0000112400002E0600002E060008000113000001130000291600002916000011250000112500002E0700002E07000800011400000114000029170000" +
		"2917000011260000112600002E0800002E080008000115000001150000291800002918000011270000112700002E0900002E09000800011600000116" +
		"00002A0100002A01000011280000112800002E0A00002E0A00080001170000011700002A0200002A02000011290000112900002E0B00002E0B000800" +
		"01180000011800002A0300002A030000112A0000112A00002E0C00002E0C00080001190000011900002A0400002A04000013010000130100002E0D00" +
		"002E0D000800011A0000011A00002A0500002A05000013020000130200002F0100002F01000800011B0000011B00002A0600002A0600001303000013" +
		"0300002F0200002F02000800011C0000011C00002A0700002A07000013040000130400002F0300002F03000800011D0000011D00002A0800002A0800" +
		"0013050000130500002F0400002F04000800011E0000011E00002A0900002A09000013060000130600002F0500002F05000800011F0000011F00002A" +
		"0A00002A0A000013070000130700002F0600002F0600080001200000012000002A0B00002A0B00001308000013080000300100003001000800012100" +
		"00012100002A0C00002A0C0000130900001309000030020000300200080001220000012200002A0D00002A0D0000130A0000130A0000300300003003" +
		"00080001230000012300002A0E00002A0E0000130B0000130B000030040000300400080001240000012400002A0F00002A0F0000130C0000130C0000" +
		"30050000300500080001250000012500002A1000002A100000130D0000130D000030060000300600080001260000012600002A1100002A110000130E" +
		"0000130E000031010000310100080001270000012700002A1200002A120000130F0000130F000031020000310200080001280000012800002A130000" +
		"2A130000131000001310000031030000310300080002010000020100002A1400002A1400001311000013110000310400003104000800020200000203" +
		"00002A1500002A1500001312000013120000320100003201000800020400000204000012010000120200001313000013130000320200003202000800" +
		"020500000205000012030000120400001314000013140000320300003203000800020600000206000012050000120600001315000013150000320400" +
		"003204000800020700000207000012070000120800001316000013160000330100003301000800020800000208000012090000120900001317000013" +
		"1700003302000033020008000209000002090000120A0000120A00001318000013180000330300003303000800020A0000020A0000120B0000120C00" +
		"001319000013190000330400003304000800020B0000020C0000120D0000120E0000131A0000131A0000330500003305000800020D0000020D000012" +
		"0F000012100000131B0000131B0000340100003401000800020E0000020E00001211000012110000131C0000131C0000340200003402000800020F00" +
		"00020F00001212000012120000131D0000131D000034030000340300080002100000021000001213000012130000131E0000131E0000350100003501" +
		"00080002110000021100001214000012150000131F0000131F0000350200003502000800021200000212000012160000121600001401000014010000" +
		"350300003503000800021300000213000012170000121800001402000014020000350400003504000800021400000214000012190000121900001403" +
		"0000140300003505000035050008000215000002150000121A0000121B000014040000140400003506000035060008000216000002160000121C0000" +
		"121D000014050000140500003601000036010008000217000002170000121E0000121E00001406000014060000360200003602000800021800000218" +
		"0000121F0000121F00001407000014070000360300003603000800021900000219000012200000122000001408000014080000360400003604000800" +
		"021A0000021A000012210000122100001409000014090000370100003701000800021B0000021B00001222000012220000140A0000140A0000370200" +
		"00370200080003010000030100001223000012230000140B0000140B000037030000370300080003020000030200001224000012240000140C000014" +
		"0C0000380100003801000800030300000303000012250000122500001501000015010000390100003901000800030400000304000012260000122600" +
		"001502000015020000390200003902000800030500000305000012270000122700001503000015030000390300003903000800030600000306000012" +
		"2800001229000015040000150400003904000039040008000307000003070000122A0000122B00001505000015050000390500003905000800030800" +
		"0003080000122C0000122C000015060000150600003906000039060008000309000003090000122D0000122D00001507000015070000390700003907" +
		"000800030A0000030A0000122E0000122F00001508000015080000390800003908000800030B0000030B000012300000123000001601000016010000" +
		"390900003909000800030C0000030D000012310000123100001602000016020000390A0000390A000800030E0000030E000012320000123200001603" +
		"000016040000390B0000390B000800030F0000030F000012330000123300001605000016050000390C0000390C000800031000000310000012340000" +
		"123600001606000016060000390D0000390D0008000311000003120000123700001237000016070000160700003A0100003A01000800031300000313" +
		"0000123800001239000016080000160800003A0200003A020008000314000003140000123A0000123B000016090000160900003A0300003A03000800" +
		"0315000003150000123C0000123D0000160A0000160A00003A0400003A040008000316000003160000123E0000123F0000160B0000160C00003A0500" +
		"003A0500080003170000031700001240000012410000160D0000160D00003B0100003B0100080003180000031800001242000012430000160E000016" +
		"0E00003B0200003B0200080003190000031900001244000012440000160F0000160F00003B0300003B03000800031A0000031A000012450000124500" +
		"0016100000161000003B0400003B04000800031B0000031B0000124600001247000016110000161200003B0500003B05000800031C0000031C000012" +
		"4800001248000016130000161400003C0100003C01000800031D0000031D0000124900001249000016150000161500003C0200003C02000800031E00" +
		"00031E0000124A0000124A000016160000161600003C0300003C03000800031F0000031F0000124B0000124C000016170000161700003D0100003D01" +
		"0008000320000003200000124D0000124D000016180000161800003D0200003D020008000321000003210000124E0100124E27001619000016190000" +
		"3D0300003D030008000322000003220000124E2800124E4800161A0000161A00003D0400003D040008000323000003230000124F0000124F0000161B" +
		"0000161B00003D0500003D0500080003240000032400001250000012500000161C0000161C00003E0101003E01010800040100000401000012510000" +
		"12520000161D0000161D00003F0101003F0101080004020000040200001253000012540000161E0000161E0000400101004001010800040300000403" +
		"00001255000012550000161F0000161F0000410100004101000800040400000404000012560000125700001620000016200000410200004102000800" +
		"040500000405000012580000125800001621000016210000410300004103000800040600000406000012590000125900001622000016220000410400" +
		"0041040008000407000004070000125A0000125A000016230000162300004105000041050008000408000004080000125B0000125B00001624000016" +
		"2400004106000041060008000409000004090000125C0000125D00001625000016250000410700004107000800040A0000040A0000125E0000125E00" +
		"001626000016260000410800004108000800040B0000040B0000125F0000126000001627000016270000410900004109000800040C0000040C000012" +
		"610000126200001628000016280000410A0000410A000800040D0000040E000012630000126500001629000016290000410B0000410B000800040F00" +
		"00040F00001266000012660000162A0000162A0000410C0000410C00080004100000041000001267000012670000162B0000162B0000410D0000410D" +
		"00080004110000041100001268000012680000162C0000162C0000410E0000410E00080004120000041200001269000012690000162D0000162D0000" +
		"410F0000410F0008000413000004130000126A0000126A0000162E0000162E00004110000041100008000414000004140000126B0000126B0000162F" +
		"0000162F00004111000041110008000415000004150000126C0000126D000016300000163000004112000041120008000416000004160000126E0000" +
		"126F00001631000016310000411300004113000800041700000417000012700000127100001632000016320000411400004114000800041800000418" +
		"000012720000127300001633000016330000411500004115000800041900000419000012740000127400001634000016340000411600004116000800" +
		"041A0000041A000012750000127600001635000016350000270100002701000800041B0000041B000012770100127718001636000016360000270200" +
		"002702000800041C0000041C000012771900127730001637000016370000270300002703000800041D0000041D000012773100127748001638000016" +
		"380000270400002704000800041E0000041E000012774900127760001639000016390000270500002705000800041F0000041F000012776100127778" +
		"00163A0000163A000027060000270600080004200000042000001277790012779000163B0000163B0000270700002707000800042100000422000012" +
		"7791001277B000163C0000163C0000270800002708000800050100000501000012780000127A0000163D0000163D0000270900002709000800050200" +
		"0005020000127B0000127D0000163E0000163E0000270A0000270A0008000503000005030000127E000012800000163F0000163F0000270B0000270B" +
		"000800050400000504000012810000128300001640000016400000270C0000270C000800050500000505000012840000128600001641000016410000" +
		"270D0000270D000800050600000506000012870000128800001642000016420000270E0000270E000800050700000507000012890000128A00001701" +
		"000017010000270F0000270F0008000508000005080000128B0000128B000017020000170200002710000027100008000509000005090000128C0000" +
		"128D00001703000017030000271100002711000800050A0000050A0000128E0000128F00001704000017040000271200002712000800050B0000050B" +
		"000012900000129000001705000017050000271300002713000800050C0000050D000012910000129100001706000017060000271400002714000800" +
		"050E0000050F000012920000129300001707000017070000271500002715000800051000000511000012940000129400001708000017080000271600" +
		"00271600080005120000051300001295000012960000170900001709000027170000271700080005140000051500002B0100002B010000170A000017" +
		"0A000027180000271800080005160000051600002B0200002B020000170B0000170B000027190000271900080005170000051700002B0300002B0300" +
		"00170C0000170C0000271A0000271A00080005180000051800002B0400002B040000170D0000170D0000271B0000271B00080006010000060100002B" +
		"0500002B050000170E0000170E0000271C0000271C00080006020000060200002B0600002B060000170F0000170F0000280100002801000800060300" +
		"00060300002B0700002B070000171000001710000028020000280200080006040000060400002B0800002B0800001711000017110000280300002803" +
		"00080006050000060500002B0900002B090000171200001712000028040000280400080006060000060600002B0A00002B0A00001713000017130000" +
		"28050000280500080006070000060700002B0B00002B0B0000171400001714000028060000280600080006080000060800002B0C00002B0C00001715" +
		"00001715000028070000280700080006090000060900002B0D00002B0D00001716000017160000280800002808000800060A0000060A00002B0E0000" +
		"2B0E00001717000017170000280900002809000800060B0000060B00002B0F00002B0F00001718000017180000280A0000280A000800060C0000060C" +
		"00002B1000002B1000001719000017190000280B0000280B000800060D0000060D00002B1100002B110000171A0000171A0000280C0000280C000800" +
		"060E0000060E00002B1200002B120000171B0000171B0000280D0000280D000800060F0000060F00002B1300002B130000171C0000171C0000280E00" +
		"00280E00080006100000061000002B1400002B140000171D0000171D0000280F0000280F00080006110000061100002B1500002B150000171E000017" +
		"1F000028100000281000080006120000061200002B1600002B160000172000001720000029010000290100080006130000061300002B1700002B1700" +
		"00172100001721000029020000290200080006140000061400002B1800002B180000172200001722000029030000290300080006150000061500002B" +
		"1900002B190000172300001723000029040000290400080007010000070100002B1A00002B1A00001724000017240000290500002905000800070200" +
		"00070200002B1B00002B1B0000172500001725000029060000290600080007030000070400002B1C00002B1C00001726000017260000290700002907" +
		"00080008010000080100002C0100002C010000172700001727000029080000290800080008020000080200002C0200002C0200001728000017280000" +
		"29090000290900080008030000080300002C0300002C0300001729000017290000290A0000290A00080008040000080400002C0400002C040000172A" +
		"0000172A0000290B0000290B00080008050000080600002C0500002C050000172B0000172B0000290C0000290C00080008070000080800002C060000" +
		"2C060000172C0000172D0000290D0000290D00080008090000080900002C0700002C070000172E0000172E0000290E0000290E000800080A0000080A" +
		"00002C0800002C080000172F0000172F0000290F0000290F000800080B0000080B00002C0900002C0900001730000017300000291000002910000800" +
		"080C0000080C00002C0A00002C0A00001731000017310000291100002911000800080D0000080D00002C0B00002C0B00001732000017320000291200" +
		"002912000800080E0000080E00002C0C00002C0C00001733000017330000291300002913000800080F0000080F00002C0D00002C0D00001734000017" +
		"34000029140000291400080008100000081000002C0E00002C0E0000180100001801000029150000291500080008110000081100002C0F00002C0F00" +
		"00180200001802000029160000291600080008120000081200002C1000002C100000180300001803000029170000291700080008130000081300002D" +
		"0100002D010000180400001804000029180000291800080008140000081400002D0200002D02000018050000180500002A0100002A01000800081500" +
		"00081600002D0300002D03000019010000190100002A0200002A0200080008170000081700002D0400002D04000019020000190200002A0300002A03" +
		"00080008180000081800002D0500002D05000019030000190300002A0400002A0400080008190000081900002D0600002D0600001904000019040000" +
		"2A0500002A05000800081A0000081A00002D0700002D07000019050000190500002A0600002A06000800081B0000081B00002D0800002D0800001906" +
		"0000190600002A0700002A07000800081C0000081C00002D0900002D09000019070000190700002A0800002A08000800081D0000081E00002D0A0000" +
		"2D0A000019080000190800002A0900002A09000800081F0000081F00002D0B00002D0B000019090000190900002A0A00002A0A000800090100000901" +
		"00002D0C00002D0C0000190A0000190A00002A0B00002A0B00080009020000090200002D0D00002D0D0000190B0000190B00002A0C00002A0C000800" +
		"09030000090300002D0E00002D0E0000190C0000190C00002A0D00002A0D00080009040000090500002D0F00002D0F0000190D0000190D00002A0E00" +
		"002A0E00080009060000090600002D1000002D100000190E0000190E00002A0F00002A0F00080009070000090700002E0100002E010000190F000019" +
		"0F00002A1000002A1000080009080000090900002E0200002E02000019100000191000002A1100002A11000800090A0000090A00002E0300002E0300" +
		"0019110000191100002A1200002A12000800090B0000090B00002E0400002E04000019120000191200002A1300002A13000800090C0000090C00002E" +
		"0500002E05000019130000191300002A1400002A14000800090D0000090D00002E0600002E06000019140000191400002A1500002A15000800090E00" +
		"00090E00002E0700002E0700001915000019150000120100001202000800090F0000090F00002E0800002E0800001916000019160000120300001204" +
		"00080009100000091000002E0900002E090000191700001917000012050000120600080009110000091100002E0A00002E0A00001918000019180000" +
		"12070000120800080009120000091200002E0B00002E0B0000191900001919000012090000120900080009130000091300002E0C00002E0C0000191A" +
		"0000191A0000120A0000120A00080009140000091400002E0D00002E0D0000191B0000191B0000120B0000120C00080009150000091500002F010000" +
		"2F010000191C0000191C0000120D0000120E00080009160000091600002F0200002F020000191D0000191D0000120F00001210000800091700000917" +
		"00002F0300002F030000191E0000191E000012110000121100080009180000091800002F0400002F040000191F0000191F0000121200001212000800" +
		"0A0100000A0100002F0500002F05000019200000192000001213000012130008000A0200000A0200002F0600002F0600001921000019210000121400" +
		"0012150008000A0300000A030000300100003001000019220000192200001216000012160008000A0400000A05000030020000300200001923000019" +
		"2300001217000012180008000A0600000A060000300300003003000019240000192400001219000012190008000A0700000A07000030040000300400" +
		"001925000019250000121A0000121B0008000A0800000A08000030050000300500001926000019260000121C0000121D0008000A0900000A09000030" +
		"060000300600001927000019270000121E0000121E0008000A0A00000A0A000031010000310100001928000019280000121F0000121F0008000A0B00" +
		"000A0B0000310200003102000019290000192900001220000012200008000A0C00000A0C00003103000031030000192A0000192A0000122100001221" +
		"0008000A0D00000A0D00003104000031040000192B0000192B00001222000012220008000A0E00000A0E00003201000032010000192C0000192C0000" +
		"1223000012230008000A0F00000A0F00003202000032020000192D0000192D00001224000012240008000A1000000A1000003203000032030000192E" +
		"0000192E00001225000012250008000A1100000A1100003204000032040000192F0000192F00001226000012260008000A1200000A12000033010000" +
		"3301000019300000193000001227000012270008000A1300000A13000033020000330200001A0100001A0100001228000012290008000A1400000A14" +
		"000033030000330300001A0200001A020000122A0000122B0008000A1500000A15000033040000330400001A0300001A030000122C0000122C000800" +
		"0A1600000A16000033050000330500001A0400001A040000122D0000122D0008000B0100000B01000034010000340100001A0500001A050000122E00" +
		"00122F0008000B0200000B02000034020000340200001A0600001A0600001230000012300008000B0300000B03000034030000340300001A0700001A" +
		"0700001231000012310008000B0400000B04000035010000350100001A0800001A0800001232000012320008000B0500000B05000035020000350200" +
		"001A0900001A0900001233000012330008000B0600000B06000035030000350300001A0A00001A0A00001234000012360008000B0700000B07000035" +
		"040000350400001A0B00001A0B00001237000012370008000B0800000B08000035050000350500001A0C00001A0C00001238000012390008000B0900" +
		"000B09000035060000350600001B0100001B010000123A0000123B0008000B0A00000B0B000036010000360100001B0200001B020000123C0000123D" +
		"0008000B0C00000B0C000036020000360200001B0300001B040000123E0000123F0008000B0D00000B0D000036030000360300001B0500001B060000" +
		"1240000012410008000B0E00000B0E000036040000360400001B0700001B0700001242000012430008000B0F00000B0F000037010000370100001B08" +
		"00001B0800001244000012440008000B1000000B10000037020000370200001B0900001B0900001245000012450008000B1100000B11000037030000" +
		"370300001B0A00001B0A00001246000012470008000B1200000B12000038010100380101001B0B00001B0B00001248000012480008000B1300000B13" +
		"000039010000390100001B0C00001B0C00001249000012490008000B1400000B14000039020000390200001B0D00001B0D0000124A0000124A000800" +
		"0B1500000B15000039030000390300001B0E00001B0E0000124B0000124C0008000B1600000B16000039040000390400001C0100001C010000124D00" +
		"00124D0008000B1700000B17000039050000390500001C0200001C020000124E0000124E0008000B1800000B18000039060000390600001C0300001C" +
		"030000124F0000124F0008000B1900000B19000039070000390700001D0100001D0100001250000012500008000C0100000C02000039080000390800" +
		"001D0200001D0200001251000012520008000C0300000C04000039090000390900001D0300001D0300001253000012540008000C0500000C06000039" +
		"0A0000390A00001D0400001D0400001255000012550008000C0700000C080000390B0000390B00001D0500001D0500001256000012560008000C0900" +
		"000C0A0000390C0000390C00001D0600001D0600001257000012580008000C0B00000C0C0000390D0000390D00001D0700001D070000125900001259" +
		"0008000C0D00000C0E00003A0100003A0100001D0800001D080000125A0000125A0008000C0F00000C0F00003A0200003A0200001D0900001D090000" +
		"125B0000125B0008000C1000000C1000003A0300003A0300001E0100001E010000125C0000125D0008000C1100000C1100003A0400003A0400001F01" +
		"00001F010000125E0000125E0008000C1200000C1200003A0500003A0500001F0200001F020000125F000012600008000C1300000C1400003B010000" +
		"3B0100001F0300001F0300001261000012620008000C1500000C1500003B0200003B0200001F0400001F0400001263000012650008000C1600000C16" +
		"00003B0300003B03000020010000200100001266000012660008000C1700000C1700003B0400003B0400002002000020020000126700001267000800" +
		"0C1800000C1900003B0500003B05000020030000200300001268000012680008000C1A00000C1B00003C0100003C0100002004000020040000126900" +
		"0012690008000C1C00000C1C00003C0200003C0200002005000020050000126A0000126A0008000C1D00000C1D00003C0300003C0300002006000020" +
		"060000126B0000126B0008000D0100000D0100003D0100003D0100002007000020070000126C0000126D0008000D0200000D0200003D0200003D0200" +
		"002101000021010000126E0000126F0008000D0300000D0400003D0300003D03000021020000210200001270000012710008000D0500000D0500003D" +
		"0400003D04000021030000210300001272000012730008000D0600000D0600003D0500003D05000022010000220100001274000012740008000D0700" +
		"000D0700003E0100003E01000022020000220200001275000012760008000D0800000D0800003F0100003F0100002203000022030000127701001277" +
		"1808000D0900000D090000400100004001000023010000230100001277190012773008000D0A00000D0A000041010000410100002302000023020000" +
		"1277310012774808000D0B00000D0C0000410200004102000023030000230300001277490012776008000D0D00000D0D000041030000410300002401" +
		"0000240100001277610012777808000D0E00000D0F0000410400004104000024020000240200001277790012779008000D1000000D10000041050000" +
		"410500002501000025010000127791001277B008000D1100000D1100004106000041060000250200002502000012780000127A0008000D1200000D12" +
		"000041070000410700002503000025030000127B0000127D0008000D1300000D14000041080000410800002504000025040000127E00001280000800" +
		"0D1500000D150000410900004109000025050000250500001281000012830008000D1600000D170000410A0000410A00002506000025060000128400" +
		"0012860008000D1800000D180000410B0000410B000025070000250700001287000012880008000D1900000D190000410C0000410C00002508000025" +
		"08000012890000128A0008000D1A00000D1A0000410D0000410D00002509000025090000128B0000128B0008000D1B00000D1C0000410E0000410E00" +
		"00250A0000250A0000128C0000128D0008000D1D00000D1D0000410F0000410F0000250B0000250B0000128E0000128E0008000D1E00000D1E000041" +
		"10000041100000250C0000250C0000128F0000128F0008000D1F00000D1F00004111000041110000250D0000250D00001290000012900008000D2000" +
		"000D2000004112000041120000250E0000250E00001291000012910008000D2100000D21000041130000411300002601000026010000129200001293" +
		"0008000D2200000D220000411400004114000026020000260200001294000012940008000D2300000D23000041150000411500002603000026030000" +
		"1295000012950008000D2400000D240000411600004116000026040000260400001296000012960000";

	public List<ReadingPlan.ReadingPlanInfo> listAllReadingPlanInfo() {
		// Add this preloaded one for tbs
		if (DatabaseUtils.queryNumEntries(helper.getReadableDatabase(), Db.TABLE_ReadingPlan) == 0) {
			final ReadingPlan.ReadingPlanInfo info = new ReadingPlan.ReadingPlanInfo();
			info.id = 1;
			info.version = 1;
			info.name = "mcheyne";
			info.title = "M'Cheyne Bible Reading Plan";
			info.description = "Daily Old Testament, New Testament, and Psalms or Gospels (www.esv.org/biblereadingplans).";
			info.duration = 365;
			info.startTime = System.currentTimeMillis();

			final byte[] data = new byte[TBS_READINGPLAN_DATA_HEX.length() / 2];
			for (int i = 0; i < data.length; i++) {
				data[i] = (byte) Integer.parseInt(TBS_READINGPLAN_DATA_HEX.substring(i * 2, i * 2 + 2), 16);
			}

			insertReadingPlan(info, data);
		}


		final Cursor c = helper.getReadableDatabase().query(Db.TABLE_ReadingPlan,
		new String[] {"_id", Db.ReadingPlan.version, Db.ReadingPlan.name, Db.ReadingPlan.title, Db.ReadingPlan.description, Db.ReadingPlan.duration, Db.ReadingPlan.startTime},
		null, null, null, null, null);
		List<ReadingPlan.ReadingPlanInfo> infos = new ArrayList<>();
		while (c.moveToNext()) {
			ReadingPlan.ReadingPlanInfo info = new ReadingPlan.ReadingPlanInfo();
			info.id = c.getLong(0);
			info.version = c.getInt(1);
			info.name = c.getString(2);
			info.title = c.getString(3);
			info.description = c.getString(4);
			info.duration = c.getInt(5);
			info.startTime = c.getLong(6);
			infos.add(info);
		}
		c.close();
		return infos;
	}

	public Pair<String, byte[]> getReadingPlanNameAndData(long _id) {
		final Cursor c = helper.getReadableDatabase().query(Db.TABLE_ReadingPlan, Array(Db.ReadingPlan.name, Db.ReadingPlan.data), "_id=?", ToStringArray(_id), null, null, null);
		try {
			if (c.moveToNext()) {
				return Pair.create(c.getString(0), c.getBlob(1));
			}
			return null;
		} finally {
			c.close();
		}
	}

	public IntArrayList getAllReadingCodesByReadingPlanProgressGid(final String gid) {
		IntArrayList res = new IntArrayList();
		try (Cursor c = helper.getReadableDatabase().query(
			Db.TABLE_ReadingPlanProgress,
			Array(Db.ReadingPlanProgress.reading_code),
			Db.ReadingPlanProgress.reading_plan_progress_gid + "=?",
			Array(gid),
			null,
			null,
			Db.ReadingPlanProgress.reading_code + " asc"
		)) {
			while (c.moveToNext()) {
				res.add(c.getInt(0));
			}
		}
		return res;
	}

	/**
	 * Deletes the reading plan, but not the progress.
	 * The progress will be kept, so it is not considered as deleted during sync.
	 */
	public void deleteReadingPlanById(long id) {
		helper.getWritableDatabase().delete(Db.TABLE_ReadingPlan, "_id=?", ToStringArray(id));

		// this removes the 'startTime' attribute from the sync entity
		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_RP);
	}

	public void updateReadingPlanStartDate(long id, long startDate) {
		final ContentValues cv = new ContentValues();
		cv.put(Db.ReadingPlan.startTime, startDate);
		helper.getWritableDatabase().update(Db.TABLE_ReadingPlan, cv, "_id=?", ToStringArray(id));

		Sync.notifySyncNeeded(SyncShadow.SYNC_SET_RP);
	}

	public List<String> listReadingPlanNames() {
		final List<String> res = new ArrayList<>();
		final Cursor c = helper.getReadableDatabase().query(Db.TABLE_ReadingPlan, new String[] {Db.ReadingPlan.name}, null, null, null, null, null);
		try {
			while (c.moveToNext()) {
				res.add(c.getString(0));
			}
			return res;
		} finally {
			c.close();
		}
	}

	@Nullable public SyncShadow getSyncShadowBySyncSetName(final String syncSetName) {
		// Getting a sync shadow that has a size bigger than 2 MB will cause crash,
		// because of system CursorWindow implementation that sets the max memory allocated
		// to be 2 MB, as defined in system resource:
		// <integer name="config_cursorWindowSize">2048</integer>
		// So we will get the size first, and then allocate memory,
		// and get the data in chunks.
		final SQLiteDatabase db = helper.getReadableDatabase();
		db.beginTransactionNonExclusive();
		try {
			final int data_len;
			final long _id;
			final int revno;

			{ // get blob len
				final Cursor c = db.rawQuery(
					"select "
						+ Table.SyncShadow.revno.name() + ", " // col 0
						+ "length(" + Table.SyncShadow.data.name() + "), " // col 1
						+ "_id " // col 2
						+ " from " + Table.SyncShadow.tableName()
						+ " where " + Table.SyncShadow.syncSetName + "=?",
					Array(syncSetName)
				);
				try {
					if (c.moveToNext()) {
						revno = c.getInt(0);
						data_len = c.getInt(1);
						_id = c.getLong(2);
					} else {
						return null;
					}
				} finally {
					c.close();
				}
			}

			final byte[] data = new byte[data_len];

			{ // fill in blob
				final int chunkSize = 1000_000;
				for (int i = 0; i < data_len; i += chunkSize) {
					final Cursor c = db.rawQuery(
						// sqlite substr func is 1-indexed
						"select "
							+ "substr(" + Table.SyncShadow.data.name() + ", " + (i + 1) + ", " + chunkSize + ")" // col 0
							+ " from " + Table.SyncShadow.tableName()
							+ " where _id=?",
						ToStringArray(_id)
					);

					try {
						if (c.moveToNext()) {
							final byte[] chunk = c.getBlob(0);
							if (i + chunk.length != data_len) {
								// not the last one
								if (chunk.length != chunkSize) {
									throw new RuntimeException("Not the requested size of chunk retrieved. data_len=" + data_len + " i=" + i + " chunk.len=" + chunk.length);
								}
								System.arraycopy(chunk, 0, data, i, chunkSize);
							} else {
								// the last one
								System.arraycopy(chunk, 0, data, i, chunk.length);
							}
						} else {
							throw new RuntimeException("Cursor moveToNext returns false, does not make sense, since previous query has indicated that this cursor has rows.");
						}
					} finally {
						c.close();
					}
				}
			}

			db.setTransactionSuccessful();

			final SyncShadow res = new SyncShadow();
			res.syncSetName = syncSetName;
			res.revno = revno;
			res.data = data;
			return res;
		} finally {
			db.endTransaction();
		}
	}

	public int getRevnoFromSyncShadowBySyncSetName(final String syncSetName) {
		final SQLiteDatabase db = helper.getReadableDatabase();
		final Cursor c = db.query(Table.SyncShadow.tableName(), Array(
			Table.SyncShadow.revno.name()
		), Table.SyncShadow.syncSetName + "=?", Array(syncSetName), null, null, null);
		try {
			if (c.moveToNext()) {
				return c.getInt(0);
			}
		} finally {
			c.close();
		}
		return 0;
	}

	@NonNull private static ContentValues syncShadowToContentValues(@NonNull final SyncShadow ss) {
		final ContentValues res = new ContentValues();
		res.put(Table.SyncShadow.syncSetName.name(), ss.syncSetName);
		res.put(Table.SyncShadow.revno.name(), ss.revno);
		res.put(Table.SyncShadow.data.name(), ss.data);
		return res;
	}

	/**
	 * Create or update a sync shadow, based on the sync set name.
	 * @param ss if the {@link yuku.alkitab.base.model.SyncShadow#syncSetName} is already on the database, this method will replace it. Otherwise, this method will insert a new one.
	 */
	public void insertOrUpdateSyncShadowBySyncSetName(@NonNull final SyncShadow ss) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		db.beginTransactionNonExclusive();
		try {
			final long count = DatabaseUtils.queryNumEntries(db, Table.SyncShadow.tableName(), Table.SyncShadow.syncSetName + "=?", Array(ss.syncSetName));
			if (count > 0) {
				db.update(Table.SyncShadow.tableName(), syncShadowToContentValues(ss), Table.SyncShadow.syncSetName + "=?", Array(ss.syncSetName));
			} else {
				db.insert(Table.SyncShadow.tableName(), null, syncShadowToContentValues(ss));
			}
			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}
	}

	public void deleteSyncShadowBySyncSetName(final String syncSetName) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		db.delete(Table.SyncShadow.tableName(), Table.SyncShadow.syncSetName + "=?", Array(syncSetName));
	}

	/**
	 * Makes the current database updated with patches (append delta) from server.
	 * Also updates the shadow (both data and the revno).
	 * @return {@link yuku.alkitab.base.sync.Sync.ApplyAppendDeltaResult#ok} if database and sync shadow are updated. Otherwise else.
	 */
	@NonNull public Sync.ApplyAppendDeltaResult applyMabelAppendDelta(final int final_revno, final List<Sync.Entity<Sync_Mabel.Content>> shadowEntities, final Sync.ClientState<Sync_Mabel.Content> clientState, @NonNull final Sync.Delta<Sync_Mabel.Content> append_delta, @NonNull final List<Sync.Entity<Sync_Mabel.Content>> entitiesBeforeSync, @NonNull final String simpleTokenBeforeSync) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		db.beginTransactionNonExclusive();
		Sync.notifySyncUpdatesOngoing(SyncShadow.SYNC_SET_MABEL, true);
		try {
			{ // if the current entities are not the same as the ones had when contacting server, reject this append delta.
				final List<Sync.Entity<Sync_Mabel.Content>> currentEntities = Sync_Mabel.getEntitiesFromCurrent();
				if (!Sync.entitiesEqual(currentEntities, entitiesBeforeSync)) {
					return Sync.ApplyAppendDeltaResult.dirty_entities;
				}
			}

			{ // if the current simpleToken has changed (sync user logged off or changed), reject this append delta
				final String simpleToken = Preferences.getString(Prefkey.sync_simpleToken);
				if (!U.equals(simpleToken, simpleTokenBeforeSync)) {
					return Sync.ApplyAppendDeltaResult.dirty_sync_account;
				}
			}

			// apply changes, which is server append delta, to current entities
			for (final Sync.Operation<Sync_Mabel.Content> o : append_delta.operations) {
				switch (o.opkind) {
					case del:
						switch (o.kind) {
							case Sync.Entity.KIND_MARKER:
								deleteMarkerByGid(o.gid);
								break;
							case Sync.Entity.KIND_LABEL:
								deleteLabelByGid(o.gid);
								break;
							case Sync.Entity.KIND_MARKER_LABEL:
								deleteMarker_LabelByGid(o.gid);
								break;
							default:
								return Sync.ApplyAppendDeltaResult.unknown_kind;
						}
						break;
					case add:
					case mod:
						switch (o.kind) {
							case Sync.Entity.KIND_MARKER:
								final Marker marker = getMarkerByGid(o.gid);
								final Marker newMarker = Sync_Mabel.updateMarkerWithEntityContent(marker, o.gid, o.content);
								insertOrUpdateMarker(newMarker);
								break;
							case Sync.Entity.KIND_LABEL:
								final Label label = getLabelByGid(o.gid);
								final Label newLabel = Sync_Mabel.updateLabelWithEntityContent(label, o.gid, o.content);
								insertOrUpdateLabel(newLabel);
								break;
							case Sync.Entity.KIND_MARKER_LABEL:
								final Marker_Label marker_label = getMarker_LabelByGid(o.gid);
								final Marker_Label newMarker_label = Sync_Mabel.updateMarker_LabelWithEntityContent(marker_label, o.gid, o.content);
								insertOrUpdateMarker_Label(newMarker_label);
								break;
							default:
								return Sync.ApplyAppendDeltaResult.unknown_kind;
						}
						break;
				}
			}

			// if we reach here, the current entities has been updated with the append delta.

			// apply changes, which are client delta, and server append delta, to shadow entities
			final List<Sync.Entity<Sync_Mabel.Content>> shadowEntitiesPatched1 = SyncAdapter.patchNoConflict(shadowEntities, clientState.delta.operations);
			final List<Sync.Entity<Sync_Mabel.Content>> shadowEntitiesPatched2 = SyncAdapter.patchNoConflict(shadowEntitiesPatched1, append_delta.operations);

			final SyncShadow ss = Sync_Mabel.shadowFromEntities(shadowEntitiesPatched2, final_revno);
			insertOrUpdateSyncShadowBySyncSetName(ss);

			db.setTransactionSuccessful();

			return Sync.ApplyAppendDeltaResult.ok;
		} finally {
			Sync.notifySyncUpdatesOngoing(SyncShadow.SYNC_SET_MABEL, false);
			db.endTransaction();
		}
	}

	/**
	 * Makes the current database updated with patches (append delta) from server.
	 * Also updates the shadow (both data and the revno).
	 * @return {@link yuku.alkitab.base.sync.Sync.ApplyAppendDeltaResult#ok} if database and sync shadow are updated. Otherwise else.
	 */
	@NonNull public Sync.ApplyAppendDeltaResult applyPinsAppendDelta(final int final_revno, @NonNull final Sync.Delta<Sync_Pins.Content> append_delta, @NonNull final List<Sync.Entity<Sync_Pins.Content>> entitiesBeforeSync, @NonNull final String simpleTokenBeforeSync) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		db.beginTransactionNonExclusive();
		Sync.notifySyncUpdatesOngoing(SyncShadow.SYNC_SET_PINS, true);
		try {
			{ // if the current entities are not the same as the ones had when contacting server, reject this append delta.
				final List<Sync.Entity<Sync_Pins.Content>> currentEntities = Sync_Pins.getEntitiesFromCurrent();
				if (!Sync.entitiesEqual(currentEntities, entitiesBeforeSync)) {
					return Sync.ApplyAppendDeltaResult.dirty_entities;
				}
			}

			{ // if the current simpleToken has changed (sync user logged off or changed), reject this append delta
				final String simpleToken = Preferences.getString(Prefkey.sync_simpleToken);
				if (!U.equals(simpleToken, simpleTokenBeforeSync)) {
					return Sync.ApplyAppendDeltaResult.dirty_sync_account;
				}
			}

			for (final Sync.Operation<Sync_Pins.Content> o : append_delta.operations) {
				switch (o.opkind) {
					case del:
					case add:
						return Sync.ApplyAppendDeltaResult.unsupported_operation;
					case mod:
						switch (o.kind) {
							case Sync.Entity.KIND_PINS: {
								// the whole logic to update all pins with the ones received from server (all pins in one entity)
								final Sync_Pins.Content content = o.content;
								final List<Sync_Pins.Content.Pin> pins = content.pins;

								for (final Sync_Pins.Content.Pin pin : pins) {
									final int preset_id = pin.preset_id;

									ProgressMark pm = getProgressMarkByPresetId(preset_id);
									if (pm == null) {
										pm = new ProgressMark();
										pm.preset_id = pin.preset_id;
									}
									pm.ari = pin.ari;
									pm.caption = pin.caption;
									pm.modifyTime = Sqlitil.toDate(pin.modifyTime);
									insertOrUpdateProgressMark(pm);
								}
							} break;
							default:
								return Sync.ApplyAppendDeltaResult.unknown_kind;
						}
						break;
				}
			}

			// if we reach here, the local database has been updated with the append delta.
			final SyncShadow ss = Sync_Pins.shadowFromEntities(Sync_Pins.getEntitiesFromCurrent(), final_revno);
			insertOrUpdateSyncShadowBySyncSetName(ss);

			db.setTransactionSuccessful();

			return Sync.ApplyAppendDeltaResult.ok;
		} finally {
			Sync.notifySyncUpdatesOngoing(SyncShadow.SYNC_SET_PINS, false);
			db.endTransaction();
		}
	}

	/**
	 * Makes the current database updated with patches (append delta) from server.
	 * Also updates the shadow (both data and the revno).
	 * @return {@link yuku.alkitab.base.sync.Sync.ApplyAppendDeltaResult#ok} if database and sync shadow are updated. Otherwise else.
	 */
	@NonNull public Sync.ApplyAppendDeltaResult applyRpAppendDelta(final int final_revno, @NonNull final Sync.Delta<Sync_Rp.Content> append_delta, @NonNull final List<Sync.Entity<Sync_Rp.Content>> entitiesBeforeSync, @NonNull final String simpleTokenBeforeSync) {
		final SQLiteDatabase db = helper.getWritableDatabase();
		db.beginTransactionNonExclusive();
		Sync.notifySyncUpdatesOngoing(SyncShadow.SYNC_SET_RP, true);
		try {
			{ // if the current entities are not the same as the ones had when contacting server, reject this append delta.
				final List<Sync.Entity<Sync_Rp.Content>> currentEntities = Sync_Rp.getEntitiesFromCurrent();
				if (!Sync.entitiesEqual(currentEntities, entitiesBeforeSync)) {
					return Sync.ApplyAppendDeltaResult.dirty_entities;
				}
			}

			{ // if the current simpleToken has changed (sync user logged off or changed), reject this append delta
				final String simpleToken = Preferences.getString(Prefkey.sync_simpleToken);
				if (!U.equals(simpleToken, simpleTokenBeforeSync)) {
					return Sync.ApplyAppendDeltaResult.dirty_sync_account;
				}
			}

			for (final Sync.Operation<Sync_Rp.Content> o : append_delta.operations) {
				if (!U.equals(o.kind, Sync.Entity.KIND_RP_PROGRESS)) {
					return Sync.ApplyAppendDeltaResult.unknown_kind;
				}

				switch (o.opkind) {
					case del: {
						db.delete(Db.TABLE_ReadingPlanProgress, Db.ReadingPlanProgress.reading_plan_progress_gid + "=?", Array(o.gid));
					} break;
					case add:
					case mod: {
						// the whole logic to update all pins with the ones received from server (all pins in one entity)
						final Sync_Rp.Content content = o.content;
						final IntArrayList readingCodes = getAllReadingCodesByReadingPlanProgressGid(o.gid);
						final TIntHashSet src = new TIntHashSet(readingCodes.size()); // our source (the current 'done' list)
						for (int i = 0, len = readingCodes.size(); i < len; i++) {
							src.add(readingCodes.get(i));
						}
						final TIntHashSet dst = new TIntHashSet(content.done); // our destination (want to be like this)

						{ // deletions
							final TIntHashSet to_del = new TIntHashSet(src);
							to_del.removeAll(dst);
							to_del.forEach(value -> {
								db.delete(Db.TABLE_ReadingPlanProgress, Db.ReadingPlanProgress.reading_plan_progress_gid + "=? and " + Db.ReadingPlanProgress.reading_code + "=?", ToStringArray(o.gid, value));
								return true;
							});
						}

						{ // additions
							final TIntHashSet to_add = new TIntHashSet(dst);
							to_add.removeAll(src);

							// unchanging properties
							final ContentValues cv = new ContentValues();
							cv.put(Db.ReadingPlanProgress.reading_plan_progress_gid, o.gid);
							cv.put(Db.ReadingPlanProgress.checkTime, System.currentTimeMillis());

							to_add.forEach(value -> {
								cv.put(Db.ReadingPlanProgress.reading_code, value);
								helper.getWritableDatabase().insert(Db.TABLE_ReadingPlanProgress, null, cv);
								return true;
							});
						}

						// update startTime
						if (content.startTime != null) {
							for (final ReadingPlan.ReadingPlanInfo info : listAllReadingPlanInfo()) {
								if (U.equals(ReadingPlan.gidFromName(info.name), o.gid)) {
									if (info.startTime != content.startTime) {
										final ContentValues cv = new ContentValues();
										cv.put(Db.ReadingPlan.startTime, content.startTime);
										db.update(Db.TABLE_ReadingPlan, cv, "_id=?", ToStringArray(info.id));
									}
									break;
								}
							}
						}
					} break;
				}
			}

			// if we reach here, the local database has been updated with the append delta.
			final SyncShadow ss = Sync_Rp.shadowFromEntities(Sync_Rp.getEntitiesFromCurrent(), final_revno);
			insertOrUpdateSyncShadowBySyncSetName(ss);

			db.setTransactionSuccessful();

			return Sync.ApplyAppendDeltaResult.ok;
		} finally {
			Sync.notifySyncUpdatesOngoing(SyncShadow.SYNC_SET_RP, false);
			db.endTransaction();
		}
	}

	/**
	 * Deletes a marker by gid.
	 * @return true when deleted.
	 */
	public boolean deleteMarkerByGid(final String gid) {
		final boolean deleted = helper.getWritableDatabase().delete(Db.TABLE_Marker, Db.Marker.gid + "=?", Array(gid)) > 0;
		if (deleted) {
			Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
		}
		return deleted;
	}

	/**
	 * Deletes a label by gid.
	 * @return true when deleted.
	 */
	public boolean deleteLabelByGid(final String gid) {
		final boolean deleted = helper.getWritableDatabase().delete(Db.TABLE_Label, Db.Label.gid + "=?", Array(gid)) > 0;
		if (deleted) {
			Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
		}
		return deleted;
	}

	/**
	 * Deletes a marker-label association by gid.
	 * @return true when deleted.
	 */
	public boolean deleteMarker_LabelByGid(final String gid) {
		final boolean deleted = helper.getWritableDatabase().delete(Db.TABLE_Marker_Label, Db.Marker_Label.gid + "=?", Array(gid)) > 0;
		if (deleted) {
			Sync.notifySyncNeeded(SyncShadow.SYNC_SET_MABEL);
		}
		return deleted;
	}

	public void insertSyncLog(final int createTime, final SyncRecorder.EventKind kind, final String syncSetName, final String params) {
		final ContentValues cv = new ContentValues(4);
		cv.put(Table.SyncLog.createTime.name(), createTime);
		cv.put(Table.SyncLog.kind.name(), kind.code);
		cv.put(Table.SyncLog.syncSetName.name(), syncSetName);
		cv.put(Table.SyncLog.params.name(), params);
		helper.getWritableDatabase().insert(Table.SyncLog.tableName(), null, cv);
	}

	public List<SyncLog> listLatestSyncLog(final int maxrows) {
		final Cursor c = helper.getReadableDatabase().query(Table.SyncLog.tableName(),
			ToStringArray(
				Table.SyncLog.createTime,
				Table.SyncLog.kind,
				Table.SyncLog.syncSetName,
				Table.SyncLog.params
			),
			null, null, null, null, Table.SyncLog.createTime + " desc", "" + maxrows);
		try {
			final List<SyncLog> res = new ArrayList<>();
			while (c.moveToNext()) {
				final SyncLog row = new SyncLog();
				row.createTime = Sqlitil.toDate(c.getInt(0));
				row.kind_code = c.getInt(1);
				row.syncSetName = c.getString(2);
				final String params_s = c.getString(3);
				if (params_s == null) {
					row.params = null;
				} else {
					row.params = App.getDefaultGson().fromJson(params_s, new TypeToken<Map<String, Object>>() {}.getType());
				}
				res.add(row);
			}
			return res;
		} finally {
			c.close();
		}
	}

	// Do not use this except in rare circumstances
	public SQLiteDatabase getWritableDatabase() {
		return helper.getWritableDatabase();
	}
}

//
//  PhDownloader.swift
//  PhDownloader Example
//
//  Created by Petrus on 7/4/20.
//  Copyright © 2020 Petrus Nguyễn Thái Học. All rights reserved.
//

import Foundation
import RxSwift
import RxRelay
import RealmSwift
import RxRealm
import RxAlamofire
import Alamofire

// MARK: - PhDownloader

public protocol PhDownloader {
  func observeState(by identifier: String) -> Observable<PhDownloadState>

  func observeState<T: Sequence>(by identifiers: T) -> Observable<[String: PhDownloadState]> where T.Element == String

  /// Download result event observable
  /// # Reference:
  /// [PhDownloadResult](x-source-tag://PhDownloadResult)
  var downloadResult$: Observable<PhDownloadResult> { get }

  /// Enqueue a download request
  func enqueue(_ request: PhDownloadRequest) -> Completable

  /// Cancel enqueued and running download task by identifier
  func cancel(by identifier: String) -> Completable

  /// Cancel all enqueued and running download tasks
  func cancelAll() -> Completable
}

/// It represents downloader errors
public enum PhDownloaderError: Error, CustomDebugStringConvertible {
  /// Realm error
  case databaseError(Error)

  /// Download error: No internet connection, file writing error, ...
  case downloadError(Error)

  /// Not found download task by identifier.
  case notFound(identifier: String)

  /// Task is not running (completed or failed). So it is can't be cancelled.
  case taskAlreadyTerminated(identifier: String)

  public var debugDescription: String {
    switch self {
    case .downloadError(let error):
      return "Download failure: \(error)."
    case .databaseError(let error):
      return "Database error: \(error)."
    case .notFound(let identifier):
      return "Not found task with identifier: \(identifier)."
    case .taskAlreadyTerminated(let identifier):
      return "Task with identifier: \(identifier) already terminated"
    }
  }
}

/// It represents downloader result in three cases: `success`, `cancelled`, `failure`
/// - Tag: PhDownloadResult
public enum PhDownloadResult {
  case success(PhDownloadRequest)
  case cancelled(PhDownloadRequest)
  case failure(PhDownloadRequest, PhDownloaderError)
}

/// Provide `PhDownloader` from `PhDownloaderOptions`
public enum PhDownloaderFactory {

  /// Provide `PhDownloader` from `PhDownloaderOptions`
  public static func makeDownloader(with options: PhDownloaderOptions) throws -> PhDownloader {
    let queue = OperationQueue()
    queue.maxConcurrentOperationCount = options.maxConcurrent * 2

    let dataSource = PhDownloadRealLocalDataSource(
      realmInitializer: provideRealm,
      queue: queue
    )

    return RealSwiftyDownloader(
      options: options,
      dataSource: dataSource
    )
  }
}

// MARK: - PhDownloadLocalDataSource

protocol PhDownloadLocalDataSource {

  /// Update download state for download task
  func update(id: String, state: PhDownloadState) -> Completable

  /// Insert or update download task
  func insertOrUpdate(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: PhDownloadState
  ) -> Completable

  /// Get `Results` by multiple ids
  func getResults(by ids: Set<String>) throws -> Results<PhDownloadTaskEntity>

  /// Get `Results` by single id
  func getResults(by id: String) throws -> Results<PhDownloadTaskEntity>

  /// Get single task by id
  func get(by id: String) throws -> PhDownloadTaskEntity?
}

extension FileManager {
  static func phDownloaderDirectory() throws -> URL {
    let url = FileManager.default
      .urls(for: .documentDirectory, in: .userDomainMask)
      .first!
      .appendingPathComponent("phDownloader", isDirectory: true)

    if !FileManager.default.fileExists(atPath: url.path) {
      try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    }

    return url
  }
}

func provideRealm() throws -> Realm {
  let fileURL = try FileManager.phDownloaderDirectory().appendingPathComponent("phdownloader_default.realm")

  let configuration = Realm.Configuration(
    fileURL: fileURL,
    // Set the new schema version. This must be greater than the previously used
    // version (if you've never set a schema version before, the version is 0).
    schemaVersion: 1,

    // Set the block which will be called automatically when opening a Realm with
    // a schema version lower than the one set above
    migrationBlock: { migration, oldSchemaVersion in
      // We haven’t migrated anything yet, so oldSchemaVersion == 0
      if (oldSchemaVersion < 1) {
        // Nothing to do!
        // Realm will automatically detect new properties and removed properties
        // And will update the schema on disk automatically
      }
    }
  )

  return try Realm(configuration: configuration)
}

public protocol RealmAdapter {
  func objects<Element: Object>(_ type: Element.Type) -> Results<Element>

  func write<Result>(
    withoutNotifying tokens: [NotificationToken],
    _ block: () throws -> Result
  ) throws -> Result

  func add(_ object: Object, update: Realm.UpdatePolicy)

  func delete<Element: Object>(_ objects: Results<Element>)

  func refresh() -> Bool

  func object<Element: Object, KeyType>(ofType type: Element.Type, forPrimaryKey key: KeyType) -> Element?
}

extension Realm: RealmAdapter { }

class PhDownloadRealLocalDataSource: PhDownloadLocalDataSource {

  /// Since we use `Realm` in background thread
  private let realmInitializer: () throws -> RealmAdapter

  /// OperationQueue that is used to dispatch blocks that updated realm `Object`
  private let queue: OperationQueue

  init(realmInitializer: @escaping () throws -> RealmAdapter, queue: OperationQueue) {
    self.realmInitializer = realmInitializer
    self.queue = queue
  }

  func update(id: String, state: PhDownloadState) -> Completable {
      .create { [queue, realmInitializer] obsever -> Disposable in
        let disposable = BooleanDisposable()

        queue.addOperation {
          autoreleasepool {
            do {
              if disposable.isDisposed { return }

              let realm = try realmInitializer()
              _ = realm.refresh()

              guard let task = Self.find(by: id, in: realm) else {
                let error = PhDownloaderError.notFound(identifier: id)
                return obsever(.error(error))
              }

              if disposable.isDisposed { return }

              try realm.write(withoutNotifying: []) {
                task.state = state.toInt
                task.updatedAt = .init()
              }

              obsever(.completed)
            } catch {
              obsever(.error(error))
            }
          }
        }

        return disposable
    }
  }

  func insertOrUpdate(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: PhDownloadState
  ) -> Completable {
      .create { [queue, realmInitializer] observer -> Disposable in
        let disposable = BooleanDisposable()

        queue.addOperation {
          autoreleasepool {
            do {
              if disposable.isDisposed { return }

              let realm = try realmInitializer()
              _ = realm.refresh()

              if disposable.isDisposed { return }

              try realm.write(withoutNotifying: []) {
                realm.add(
                  PhDownloadTaskEntity(
                    identifier: identifier,
                    url: url,
                    fileName: fileName,
                    savedDir: savedDir,
                    state: state.toInt,
                    updatedAt: .init()
                  ),
                  update: .modified
                )
              }

              observer(.completed)
            } catch {
              observer(.error(error))
            }
          }
        }

        return disposable
    }
  }

  func getResults(by ids: Set<String>)throws -> Results<PhDownloadTaskEntity> {
    try self.realmInitializer()
      .objects(PhDownloadTaskEntity.self)
      .filter("SELF.identifier IN %@", ids)
  }

  func getResults(by id: String)throws -> Results<PhDownloadTaskEntity> {
    try self.realmInitializer()
      .objects(PhDownloadTaskEntity.self)
      .filter("SELF.identifier = %@", id)
  }

  func get(by id: String) throws -> PhDownloadTaskEntity? {
    Self.find(by: id, in: try self.realmInitializer())
  }

  private static func find(by id: String, in realm: RealmAdapter) -> PhDownloadTaskEntity? {
    realm.object(ofType: PhDownloadTaskEntity.self, forPrimaryKey: id)
  }
}

class PhDownloadTaskEntity: Object {
  @objc dynamic var identifier: String = ""
  @objc dynamic var url: String = ""
  @objc dynamic var fileName: String = ""
  @objc dynamic var savedDir: String = ""
  @objc dynamic var state: Int = undefined
  @objc dynamic var updatedAt: Date = .init()

  override class func primaryKey() -> String? { "identifier" }

  // MARK: Integer constants as download state

  static let undefined = -2
  static let enqueued = -1
  static let completed = 101
  static let failed = 102
  static let cancelled = 103

  convenience init(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: Int,
    updatedAt: Date
  ) {
    self.init()
    self.identifier = identifier
    self.url = url.absoluteString
    self.fileName = fileName
    self.savedDir = savedDir.path
    self.state = state
    self.updatedAt = updatedAt
  }
}

extension PhDownloadState {
  var toInt: Int {
    switch self {
    case .undefined:
      return PhDownloadTaskEntity.undefined
    case .enqueued:
      return PhDownloadTaskEntity.enqueued
    case .downloading(let progress):
      return progress
    case .completed:
      return PhDownloadTaskEntity.completed
    case .failed:
      return PhDownloadTaskEntity.failed
    case .cancelled:
      return PhDownloadTaskEntity.cancelled
    }
  }
}

extension Int {
  var toDownloadState: PhDownloadState {
    switch self {
    case PhDownloadTaskEntity.undefined:
      return .undefined
    case PhDownloadTaskEntity.enqueued:
      return .enqueued
    case 0...100:
      return .downloading(progress: self)
    case PhDownloadTaskEntity.completed:
      return .completed
    case PhDownloadTaskEntity.failed:
      return .failed
    case PhDownloadTaskEntity.cancelled:
      return .cancelled
    default:
      fatalError("Invalid state: \(self)")
    }
  }
}

extension RxProgress {
  var asDownloadState: PhDownloadState {
    if self.totalBytes <= 0 { return .downloading(progress: 0) }
    let percent = Double(self.bytesWritten) / Double(self.totalBytes)
    return .downloading(progress: Int(100 * percent))
  }
}

extension Event where Element == RxProgress {
  func asDownloadState(_ isCompleted: Bool) -> PhDownloadState {
    switch self {
    case .next(let progress):
      return progress.asDownloadState
    case .error:
      return .failed
    case .completed:
      return isCompleted ? .completed : .cancelled
    }
  }
}

/// The command that enqueues a download request or cancel by identifier
enum Command {
  case enqueue(request: PhDownloadRequest)
  case cancel(identifier: String)
}

// MARK: - RealSwiftyDownloader

final class RealSwiftyDownloader: PhDownloader {
  // MARK: Dependencies
  private let options: PhDownloaderOptions
  private let dataSource: PhDownloadLocalDataSource

  // MARK: ReactiveX
  private let commandS = PublishRelay<Command>()
  private let downloadResultS = PublishRelay<PhDownloadResult>()
  private let disposeBag = DisposeBag()

  // MARK: Schedulers
  private let throttleScheduler = SerialDispatchQueueScheduler(qos: .utility)
  private static var mainScheduler: MainScheduler { .instance }
  private static var concurrentMainScheduler: ConcurrentMainScheduler { .instance }

  internal init(options: PhDownloaderOptions, dataSource: PhDownloadLocalDataSource) {
    self.options = options
    self.dataSource = dataSource

    self
      .commandS
      .compactMap {
        if case .enqueue(request: let request) = $0 { return request }
        return nil
      }
      .map { [weak self] request in self?.executeDownload(request) ?? .empty() }
      .merge(maxConcurrent: options.maxConcurrent)
      .subscribe()
      .disposed(by: self.disposeBag)
  }

  // MARK: Private helpers

  /// Execute download request, update local database and send result.
  /// All errors is sent to `downloadResultS` and  is discarded afterwards.
  /// - Parameter request: Download request
  /// - Returns: an observable that emits `Void`
  private func executeDownload(_ request: PhDownloadRequest) -> Observable<Void> {
    var isCompleted = false

    return Observable
      .deferred { [dataSource] () -> Observable<DownloadRequest> in
        // already cancelled before
        if (try dataSource.get(by: request.identifier))?.state.toDownloadState == .cancelled {
          return .empty()
        }

        let urlRequest = URLRequest(url: request.url)
        let destination: DownloadRequest.Destination = { (temporaryURL, response) in
          (
            request.savedDir.appendingPathComponent(request.fileName),
            [.createIntermediateDirectories, .removePreviousFile]
          )
        }
        return RxAlamofire.download(urlRequest, to: destination)
      }
      .subscribeOn(Self.concurrentMainScheduler)
      .flatMap { $0.rx.progress() }
      .observeOn(Self.mainScheduler)
      .do(onCompleted: { isCompleted = true })
      .takeUntil(self.cancelCommand(for: request.identifier))
      .throttle(self.options.throttleProgress, latest: true, scheduler: self.throttleScheduler)
      .distinctUntilChanged()
      .materialize()
      .observeOn(Self.mainScheduler)
      .map { (state: $0.asDownloadState(isCompleted), error: $0.error) }
      .do(
        onNext: { [downloadResultS] (state, error) in
          if case .completed = state {
            downloadResultS.accept(.success(request))
          }
          else if case .cancelled = state {
            downloadResultS.accept(.cancelled(request))
          }
          else if case .failed = state, let error = error {
            downloadResultS.accept(.failure(request, .downloadError(error)))
          }
        }
      )
      .concatMap { [dataSource] (state, _) -> Completable in
        dataSource.update(
          id: request.identifier,
          state: state
        )
      }
      .map { _ in () }
      .catchError { error in
        print("[PhDownloader] Unhandle error: \(error)")
        return .empty()
      }
      .asObservable() // pretty format :)
  }

  /// Filter command cancel task that has id equals to `identifierNeedCancel`
  private func cancelCommand(for identifierNeedCancel: String) -> Observable<Void> {
    self.commandS.compactMap {
      if case .cancel(let identifier) = $0, identifier == identifierNeedCancel { return () }
      return nil
    }
  }

  /// Sequential execute:
  /// * Update file system: remove dowloaded file.
  /// * Update local database: Update task state to cancelled
  private func cancelDownload(_ identifier: String) -> Completable {
      .create { observer -> Disposable in
        let disposable = BooleanDisposable()
        let compositeDisposable = CompositeDisposable()
        _ = compositeDisposable.insert(disposable)

        DispatchQueue.main.async {
          do {
            if disposable.isDisposed { return }

            guard let task = try self.dataSource.get(by: identifier) else {
              return observer(.error(PhDownloaderError.notFound(identifier: identifier)))
            }

            guard !Set<PhDownloadState>([.completed, .failed]).contains(task.state.toDownloadState) else {
              return observer(.error(PhDownloaderError.taskAlreadyTerminated(identifier: identifier)))
            }

            if disposable.isDisposed { return }

            let fileURL = URL(fileURLWithPath: task.savedDir).appendingPathComponent(task.fileName)
            try? FileManager.default.removeItem(at: fileURL)

            if disposable.isDisposed { return }

            _ = compositeDisposable.insert(
              self.dataSource
                .update(id: identifier, state: .cancelled)
                .subscribe(observer)
            )
          } catch {
            observer(.error(PhDownloaderError.databaseError(error)))
          }
        }

        return compositeDisposable
    }
  }
}

// MARK: Observe state by identifier
extension RealSwiftyDownloader {
  func observeState(by identifier: String) -> Observable<PhDownloadState> {
    Observable
      .deferred { [dataSource] () -> Observable<PhDownloadState> in
        do {
          if let task = try dataSource.get(by: identifier) {
            return Observable
              .from(object: task, emitInitialValue: true)
              .map { $0.state.toDownloadState }
              .subscribeOn(Self.concurrentMainScheduler)
          }

          return Observable
            .collection(
              from: try dataSource.getResults(by: identifier),
              synchronousStart: true,
              on: .main
            )
            .map { results in results.first?.state.toDownloadState ?? .undefined }
            .subscribeOn(Self.concurrentMainScheduler)

        } catch {
          return .error(PhDownloaderError.databaseError(error))
        }
      }
      .distinctUntilChanged()
  }

}

// MARK: Observe state by identifiers
extension RealSwiftyDownloader {
  func observeState<T: Sequence>(by identifiers: T) -> Observable<[String: PhDownloadState]> where T.Element == String {
      .deferred { [dataSource] () -> Observable<[String : PhDownloadState]> in
        do {
          return Observable
            .collection(
              from: try dataSource.getResults(by: Set(identifiers)),
              synchronousStart: true,
              on: .main
            )
            .subscribeOn(Self.concurrentMainScheduler)
            .map { results in
              let tuples = Array(results).map { ($0.identifier, $0.state.toDownloadState) }
              let stateById = Dictionary(uniqueKeysWithValues: tuples)
              return Dictionary(uniqueKeysWithValues: identifiers.map { ($0, stateById[$0] ?? .undefined) })
            }
            .distinctUntilChanged()
        } catch {
          return .error(PhDownloaderError.databaseError(error))
        }
    }
  }
}

// MARK: Download result observable
extension RealSwiftyDownloader {
  var downloadResult$: Observable<PhDownloadResult> { self.downloadResultS.asObservable() }
}

// MARK: Enqueue download request
extension RealSwiftyDownloader {
  func enqueue(_ request: PhDownloadRequest) -> Completable {
    // insert or update task into database
    // and then, send command to enqueue download request
    self.dataSource
      .insertOrUpdate(
        identifier: request.identifier,
        url: request.url,
        fileName: request.fileName,
        savedDir: request.savedDir,
        state: .enqueued
      )
      .observeOn(Self.mainScheduler)
      .do(onCompleted: { [commandS] in commandS.accept(.enqueue(request: request)) })
  }
}

// MARK: Cancel by identifier
extension RealSwiftyDownloader {
  func cancel(by identifier: String) -> Completable {
    // mask task as cancelled to prevent executing enqueued task
    // and then, send command to cancel downloading task
    self.cancelDownload(identifier)
      .observeOn(Self.mainScheduler)
      .do(onCompleted: { [commandS] in commandS.accept(.cancel(identifier: identifier)) })
  }
}

// MARK: Cancel all
extension RealSwiftyDownloader {
  func cancelAll() -> Completable {
    fatalError()
  }
}
